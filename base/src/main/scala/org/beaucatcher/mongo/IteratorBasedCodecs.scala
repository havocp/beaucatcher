package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * This trait allows you to quickly implement a set of codecs by
 * providing methods to convert to/from Iterator[(String,Any)].
 */
trait IteratorBasedCodecs[T] extends IdEncoders with ValueDecoders {
    import CodecUtils._

    // TODO rename these to be iteratorBasedQueryEncoder, etc.
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

trait CollectionCodecSetEntityCodecsIteratorBased[EntityType]
    extends CollectionCodecSetEntityCodecs[EntityType] {
    self: CollectionCodecSet[_, EntityType, EntityType, _, _] =>

    protected def toIterator(t: EntityType): Iterator[(String, Any)]

    protected def fromIterator(i: Iterator[(String, Any)]): EntityType

    private def outerToIterator = toIterator(_)
    private def outerFromIterator = fromIterator(_)

    private object Codecs extends IteratorBasedCodecs[EntityType] {
        override def toIterator(t: EntityType): Iterator[(String, Any)] =
            outerToIterator(t)
        override def fromIterator(i: Iterator[(String, Any)]): EntityType =
            outerFromIterator(i)
    }

    override implicit def collectionQueryResultDecoder: QueryResultDecoder[EntityType] =
        Codecs.queryResultDecoder
    override implicit def collectionModifierEncoderEntity: ModifierEncoder[EntityType] =
        Codecs.modifierEncoder
    override implicit def collectionUpdateQueryEncoder: UpdateQueryEncoder[EntityType] =
        Codecs.updateQueryEncoder
    override implicit def collectionUpsertEncoder: UpsertEncoder[EntityType] =
        Codecs.upsertEncoder
}
