package org.beaucatcher.mongo

import java.nio.ByteBuffer
import scala.annotation.implicitNotFound

/** Trait means we can write a BSON document to an EncodeBuffer. */
trait DocumentEncoder[-T] {
    def encode(buf: EncodeBuffer, t: T): Unit
    /**
     * Encode as an iterator where the values are
     * Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
     * java.util.Date, Seq[Any], or T.
     * Each call to this must create a new iterator.
     */
    def encodeIterator(t: T): Iterator[(String, Any)]
}

/** Extend this to implement encode() automatically if you implement encodeIterator() */
trait IteratorBasedDocumentEncoder[-T] extends DocumentEncoder[T] {
    import CodecUtils._

    private val fieldWriter: FieldWriter = {
        case (buf, name, doc: Iterator[_]) =>
            writeFieldDocument(buf, name, doc.asInstanceOf[Iterator[(String, Any)]])(IteratorCodecs.iteratorQueryEncoder)
    }

    override def encode(buf: EncodeBuffer, t: T): Unit = {
        val start = writeOpenDocument(buf)

        for (field <- encodeIterator(t)) {
            writeValueAny(buf, field._1, field._2, fieldWriter)
        }

        writeCloseDocument(buf, start)
    }
}

/** Trait means we can read a BSON document from a DecodeBuffer. */
trait DocumentDecoder[+T] {
    def decode(buf: DecodeBuffer): T
    /**
     * Decode from an iterator where the values are
     * Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
     * java.util.Date, Seq[Any], or nested Iterator[(String, Any)].
     */
    def decodeIterator(iterator: Iterator[(String, Any)]): T
}

/** Extend this to implement decode() automatically if you implement decodeIterator() */
trait IteratorBasedDocumentDecoder[+T] extends DocumentDecoder[T] {
    import CodecUtils._

    def decode(buf: DecodeBuffer): T = {
        decodeIterator(IteratorCodecs.iteratorQueryResultDecoder.decode(buf))
    }
}

/** Trait means we can convert a single BSON value from a DecodeBuffer. */
@implicitNotFound("Can't find an implicit ValueDecoder to read values into a ${T}")
trait ValueDecoder[+T] {
    /**
     * The code is the BSON typecode and the buffer
     * should be positioned just after that code.
     * The ValueDecoder must consume the entire value
     * and nothing more.
     */
    def decode(code: Byte, buf: DecodeBuffer): T
    /**
     * Decode a value of type Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
     * java.util.Date, Seq[Any], or Iterator[(String, Any)]
     */
    def decodeAny(value: Any): T
}

/** Implement this to get decode() free for implementing decodeAny() */
trait IteratorBasedValueDecoder[+T] extends ValueDecoder[T] {
    import CodecUtils._

    override def decode(code: Byte, buf: DecodeBuffer): T = {
        import IteratorCodecs._
        decodeAny(readAny[Iterator[(String, Any)]](code, buf))
    }
}

/** Trait means we can encode as an ID field */
@implicitNotFound("Can't find an implicit IdEncoder to encode ${T} as a document ID")
trait IdEncoder[-T] {
    // this encodes just the one field (typecode, name, value), not a document.
    def encodeField(buf: EncodeBuffer, name: String, t: T): Unit
    /**
     * Encode an ID of type Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
     * java.util.Date
     */
    def encodeFieldAny(t: T): Any
}

/**
 * Extend this to automatically get encodeField() if you implement
 * encodeFieldAny()
 */
trait IteratorBasedIdEncoder[-T] extends IdEncoder[T] {

    override def encodeField(buf: EncodeBuffer, name: String, t: T): Unit = {
        import CodecUtils._
        writeValueAny(buf, name, encodeFieldAny(t), { case _ if false => }: FieldWriter)
    }
}

/** If an implicit QueryEncoder exists for a type, that type can be encoded into a query document. */
@implicitNotFound("Can't find an implicit QueryEncoder to convert ${T} into a MongoDB query")
trait QueryEncoder[-T] extends DocumentEncoder[T] {

}

/** If an implicit QueryResultDecoder exists for a type, that type can be decoded into a query result document. */
@implicitNotFound("Can't find an implicit QueryResultDecoder to convert ${T} from a MongoDB document")
trait QueryResultDecoder[+T] extends DocumentDecoder[T] {

}

/**
 * If an implicit UpdateQueryEncoder exists for a type, that type can be encoded into a query suitable
 * for an update. If the type has an _id then an update query could generally include _only_ the ID,
 * while if the type does not the whole query would be retained.
 */
@implicitNotFound("Can't find an implicit UpdateQueryEncoder to convert ${T} into a query to update itself.")
trait UpdateQueryEncoder[-T] extends DocumentEncoder[T] {

}

/**
 * If an implicit ModifierEncoder exists for a type, that type can be encoded into a query that
 * modifies documents. A modifier can be a subset of the fields in the target document,
 * must not contain the "_id" field, and could also contain special operators like "$$inc".
 */
@implicitNotFound("Can't find an implicit ModifierEncoder to convert ${T} into a modifier query.")
trait ModifierEncoder[-T] extends DocumentEncoder[T] {

}

/**
 * If an implicit UpsertEncoder exists for a type, that type can be encoded as a Modifier document
 * when performing an upsert. This modifier document could have the "_id" in it.
 * This encoder is also used for plain inserts.
 */
@implicitNotFound("Can't find an implicit UpsertEncoder to convert ${T} into an upsertable document.")
trait UpsertEncoder[-T] extends DocumentEncoder[T] {

}

/** An empty document, which can be encoded with no knowledge other than its emptiness. */
sealed trait EmptyDocument

object EmptyDocument extends EmptyDocument {
    implicit def queryEncoder: QueryEncoder[EmptyDocument] =
        _queryEncoder

    object _queryEncoder
        extends QueryEncoder[EmptyDocument] {
        override def encode(buf: EncodeBuffer, doc: EmptyDocument): Unit = {
            // length 5 empty document is 4 bytes for length, plus a nul byte
            buf.writeInt(5)
            buf.writeByte(0)
        }
        override def encodeIterator(doc: EmptyDocument): Iterator[(String, Any)] = {
            Iterator.empty
        }
    }
}

/** A document which should never be decoded (it throws if it is). */
sealed trait ErrorIfDecodedDocument

object ErrorIfDecodedDocument extends ErrorIfDecodedDocument {
    implicit def queryResultDecoder: QueryResultDecoder[ErrorIfDecodedDocument] =
        _queryResultDecoder

    private object _queryResultDecoder
        extends QueryResultDecoder[ErrorIfDecodedDocument] {
        private def fail[U]: U =
            throw new BugInSomethingMongoException("ErrorIfDecodedDocument was decoded, so here is your error (this type is used to assert that it's never decoded)")

        override def decode(buf: DecodeBuffer): ErrorIfDecodedDocument =
            fail

        override def decodeIterator(iterator: Iterator[(String, Any)]): ErrorIfDecodedDocument =
            fail
    }
}

/** A document which should never be decoded (it throws if it is). */
sealed trait ErrorIfDecodedValue

object ErrorIfDecodedValue extends ErrorIfDecodedValue {
    implicit def valueDecoder: ValueDecoder[ErrorIfDecodedValue] =
        _valueDecoder

    private object _valueDecoder
        extends ValueDecoder[ErrorIfDecodedValue] {
        private def fail[U]: U =
            throw new BugInSomethingMongoException("ErrorIfDecodedValue was decoded, so here is your error (this type is used to assert that it's never decoded)")

        override def decode(code: Byte, buf: DecodeBuffer): ErrorIfDecodedValue =
            fail

        override def decodeAny(v: Any): ErrorIfDecodedValue =
            fail
    }
}
