package org.beaucatcher.mongo

import java.nio.ByteBuffer
import scala.annotation.implicitNotFound

/** Trait means we can write a BSON document to an EncodeBuffer. */
trait DocumentEncoder[-T] {
    def encode(buf: EncodeBuffer, t: T): Unit
}

/** Trait means we can read a BSON document from a DecodeBuffer. */
trait DocumentDecoder[+T] {
    def decode(buf: DecodeBuffer): T
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
}

/** Trait means we can encode as an ID field */
@implicitNotFound("Can't find an implicit IdEncoder to encode ${T} as a document ID")
trait IdEncoder[-T] {
    // this encodes just the one field (typecode, name, value), not a document.
    def encodeField(buf: EncodeBuffer, name: String, t: T): Unit
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
 * must not contain the "_id" field, and could also contain special operators like "$inc".
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
object EmptyDocument {
    implicit object emptyDocumentQueryEncoder
        extends QueryEncoder[EmptyDocument.type] {
        override def encode(buf: EncodeBuffer, doc: EmptyDocument.type): Unit = {
            // length 5 empty document is 4 bytes for length, plus a nul byte
            buf.writeInt(5)
            buf.writeByte(0)
        }
    }
}
