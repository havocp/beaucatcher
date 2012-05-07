package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * This trait allows you to quickly implement a set of codecs by
 * providing methods to convert to/from Any, as with ValueDecoder.decodeAny.
 */
trait IteratorBasedCodecs[T] extends IdEncoders with ValueDecoders {
    import CodecUtils._

    private lazy val _anyValueDecoder =
        ValueDecoders.anyValueDecoder[T]()

    // can't be implicit since it would always apply
    def anyValueDecoder: ValueDecoder[Any] =
        _anyValueDecoder

    implicit def queryEncoder: QueryEncoder[T] =
        UnmodifiedEncoder

    implicit def queryResultDecoder: QueryResultDecoder[T] =
        Decoder

    implicit def updateQueryEncoder: UpdateQueryEncoder[T] =
        OnlyIdEncoder

    implicit def modifierEncoder: ModifierEncoder[T] =
        WithoutIdEncoder

    implicit def upsertEncoder: UpsertEncoder[T] =
        UnmodifiedEncoder

    protected def toIterator(t: T): Iterator[(String, Any)]

    protected def fromIterator(i: Iterator[(String, Any)]): T

    private object UnmodifiedEncoder
        extends IteratorBasedDocumentEncoder[T]
        with QueryEncoder[T]
        with UpsertEncoder[T] {

        override def encodeIterator(t: T): Iterator[(String, Any)] = {
            toIterator(t)
        }
    }

    private object WithoutIdEncoder
        extends IteratorBasedDocumentEncoder[T]
        with ModifierEncoder[T] {
        override def encodeIterator(t: T): Iterator[(String, Any)] = {
            toIterator(t).filter({ field => field._1 != "_id" })
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private object OnlyIdEncoder
        extends IteratorBasedDocumentEncoder[T]
        with UpdateQueryEncoder[T] {
        override def encodeIterator(t: T): Iterator[(String, Any)] = {
            val id = toIterator(t)
                .find({ field => field._1 == "_id" })
                .getOrElse(throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)"))
                ._2
            Iterator("_id" -> id)
        }
    }

    private object Decoder
        extends IteratorBasedDocumentDecoder[T]
        with QueryResultDecoder[T] {
        override def decodeIterator(iterator: Iterator[(String, Any)]): T = {
            fromIterator(iterator)
        }
    }
}
