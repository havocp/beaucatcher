package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * This trait allows you to quickly implement a set of codecs by
 * providing methods to convert to/from Map[String,Any].
 */
trait MapBasedCodecs[T] extends IdEncoders with ValueDecoders {
    import CodecUtils._

    implicit def mapBasedQueryEncoder: QueryEncoder[T] =
        UnmodifiedEncoder

    implicit def mapBasedQueryResultDecoder: QueryResultDecoder[T] =
        Decoder

    implicit def mapBasedUpdateQueryEncoder: UpdateQueryEncoder[T] =
        OnlyIdEncoder

    implicit def mapBasedModifierEncoder: ModifierEncoder[T] =
        WithoutIdEncoder

    implicit def mapBasedUpsertEncoder: UpsertEncoder[T] =
        UnmodifiedEncoder

    protected def toMap(t: T): Map[String, Any]

    protected def fromMap(i: Map[String, Any]): T

    private object UnmodifiedEncoder
        extends IteratorBasedDocumentEncoder[T]
        with QueryEncoder[T]
        with UpsertEncoder[T] {

        override def encodeIterator(t: T): Iterator[(String, Any)] = {
            MapCodecs.mapToIterator(toMap(t))
        }
    }

    private object WithoutIdEncoder
        extends IteratorBasedDocumentEncoder[T]
        with ModifierEncoder[T] {
        override def encodeIterator(t: T): Iterator[(String, Any)] = {
            toMap(t).filterKeys(_ != "_id").iterator
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private object OnlyIdEncoder
        extends IteratorBasedDocumentEncoder[T]
        with UpdateQueryEncoder[T] {
        override def encodeIterator(t: T): Iterator[(String, Any)] = {
            Iterator("_id" -> toMap(t).getOrElse("_id", throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)")))
        }
    }

    private object Decoder
        extends IteratorBasedDocumentDecoder[T]
        with QueryResultDecoder[T] {
        override def decodeIterator(iterator: Iterator[(String, Any)]): T = {
            fromMap(MapCodecs.iteratorToMap(iterator))
        }
    }
}

trait CollectionCodecSetEntityCodecsMapBased[EntityType]
    extends CollectionCodecSetEntityCodecs[EntityType] {
    self: CollectionCodecSet[_, EntityType, EntityType, _, _] =>

    protected def toMap(t: EntityType): Map[String, Any]

    protected def fromMap(m: Map[String, Any]): EntityType

    private def outerToMap = toMap(_)
    private def outerFromMap = fromMap(_)

    private object Codecs extends MapBasedCodecs[EntityType] {
        override protected def toMap(t: EntityType): Map[String, Any] =
            outerToMap(t)

        override protected def fromMap(m: Map[String, Any]): EntityType =
            outerFromMap(m)
    }

    override implicit def collectionQueryResultDecoder: QueryResultDecoder[EntityType] =
        Codecs.mapBasedQueryResultDecoder
    override implicit def collectionModifierEncoderEntity: ModifierEncoder[EntityType] =
        Codecs.mapBasedModifierEncoder
    override implicit def collectionUpdateQueryEncoder: UpdateQueryEncoder[EntityType] =
        Codecs.mapBasedUpdateQueryEncoder
    override implicit def collectionUpsertEncoder: UpsertEncoder[EntityType] =
        Codecs.mapBasedUpsertEncoder
}
