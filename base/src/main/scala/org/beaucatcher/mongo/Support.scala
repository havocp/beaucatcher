package org.beaucatcher.mongo

import java.nio.ByteBuffer
import scala.annotation.implicitNotFound

trait EncodeSupport[-T] {
    def encode(t: T): ByteBuffer
}

trait DecodeSupport[+T] {
    def decode(buf: ByteBuffer): T
}

/** If an implicit QueryEncodeSupport exists for a type, that type can be encoded into a query document. */
@implicitNotFound("Can't find an implicit QueryEncodeSupport to convert ${T} into a MongoDB query")
trait QueryEncodeSupport[-T] extends EncodeSupport[T] {

}

/** If an implicit EntityDecodeSupport exists for a type, that type can be decoded into a query result document. */
@implicitNotFound("Can't find an implicit EntityDecodeSupport to convert ${T} from a MongoDB document")
trait EntityDecodeSupport[+T] extends DecodeSupport[T] {

}

/** If an implicit EntityEncodeSupport exists for a type, that type can be encoded into an update or insert document. */
@implicitNotFound("Can't find an implicit EntityEncodeSupport to convert ${T} to a MongoDB document")
trait EntityEncodeSupport[-T] extends EncodeSupport[T] {

}

/** Marks an empty document, which can be encoded with no knowledge other than its emptiness */
trait EmptyDocument

/** An instance of the EmptyDocument trait */
object EmptyDocument extends EmptyDocument
