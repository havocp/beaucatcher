package org.beaucatcher.mongo

import java.nio.ByteBuffer
import scala.annotation.implicitNotFound

/** Trait means we can convert a BSON document to a ByteBuffer. */
trait DocumentEncoder[-T] {
    def encode(buf: EncodeBuffer, t: T): Unit
}

/** Trait means we can convert a BSON document from a ByteBuffer. */
trait DocumentDecoder[+T] {
    def decode(buf: DecodeBuffer): T
}

/** Trait means we can convert a single BSON value from a ByteBuffer. */
@implicitNotFound("Can't find an implicit ValueDecoder to read values into a ${T}")
trait ValueDecoder[+T] {
    /**
     * The code is the BSON typecode and the buffer
     * should be positioned just after that code.
     * The ValueDecoder must consume the entire value
     * and nothing more.
     */
    def decode(code: Byte, buf: ByteBuffer): T
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
 * If an implicit UpdateQueryEncoder exists for a type, that type can be encoded into a query that
 *  matches only the provided instance; typically the encoded document has just the ID in it.
 */
@implicitNotFound("Can't find an implicit UpdateQueryEncoder to convert ${T} into a query to update itself.")
trait UpdateQueryEncoder[-T] extends DocumentEncoder[T] {

}

/**
 * If an implicit ModifierEncoder exists for a type, that type can be encoded into a query that
 * modifies documents. A modifier can be a subset of the fields in the target document, generally
 * should not contain the "_id" field, and could also contain special operators like "$inc".
 */
@implicitNotFound("Can't find an implicit ModifierEncoder to convert ${T} into a modifier query.")
trait ModifierEncoder[-T] extends DocumentEncoder[T] {

}

/**
 * If an implicit UpsertEncoder exists for a type, that type can be encoded into a query that
 * matches only the provided instance and provides a new value for that instance. Typically
 * an encoded upsertable document is the entire document with all fields including ID.
 */
@implicitNotFound("Can't find an implicit UpsertEncoder to convert ${T} into an upsertable document.")
trait UpsertEncoder[-T] extends DocumentEncoder[T] {

}

/** A compat type that we'll get rid of and split into the above specifics. */
@implicitNotFound("Can't find an implicit EntityEncodSupport to convert ${T} into an entity.")
trait EntityEncodeSupport[-T] extends DocumentEncoder[T] {

}

/** Marks an empty document, which can be encoded with no knowledge other than its emptiness */
trait EmptyDocument

/** An instance of the EmptyDocument trait */
object EmptyDocument extends EmptyDocument
