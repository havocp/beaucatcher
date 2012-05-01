package org.beaucatcher.mongo

import scala.annotation.implicitNotFound

/** The set of encoders and decoders required to read from a collection */
@implicitNotFound("Can't find a codec set for a read-only collection using ${QueryType} for queries, ${EntityType} for results, ${IdType} for IDs, and ${ValueType} for distinct values")
trait ReadOnlyCollectionCodecSet[QueryType, EntityType, IdType, ValueType] {
    implicit def collectionValueDecoder: ValueDecoder[ValueType]
    implicit def collectionQueryEncoder: QueryEncoder[QueryType]
    implicit def collectionIdEncoder: IdEncoder[IdType]
    implicit def collectionQueryResultDecoder: QueryResultDecoder[EntityType]
}

private case class ReadOnlyCollectionCodecSetImpl[QueryType, EntityType, IdType, ValueType](
    override val collectionValueDecoder: ValueDecoder[ValueType],
    override val collectionQueryEncoder: QueryEncoder[QueryType],
    override val collectionIdEncoder: IdEncoder[IdType],
    override val collectionQueryResultDecoder: QueryResultDecoder[EntityType])
    extends ReadOnlyCollectionCodecSet[QueryType, EntityType, IdType, ValueType]

object ReadOnlyCollectionCodecSet {
    def apply[QueryType, EntityType, IdType, ValueType]()(implicit collectionValueDecoder: ValueDecoder[ValueType],
        collectionQueryEncoder: QueryEncoder[QueryType],
        collectionIdEncoder: IdEncoder[IdType],
        collectionQueryResultDecoder: QueryResultDecoder[EntityType]): ReadOnlyCollectionCodecSet[QueryType, EntityType, IdType, ValueType] =
        ReadOnlyCollectionCodecSetImpl[QueryType, EntityType, IdType, ValueType](collectionValueDecoder,
            collectionQueryEncoder, collectionIdEncoder, collectionQueryResultDecoder)
}

/** The set of encoders and decoders required to read and write a collection */
@implicitNotFound("Can't find a codec set for a collection using ${QueryType} for queries, ${EntityType} for results, ${IdType} for IDs, and ${ValueType} for distinct values")
trait CollectionCodecSet[QueryType, EntityType, IdType, ValueType]
    extends ReadOnlyCollectionCodecSet[QueryType, EntityType, IdType, ValueType] {

    implicit def collectionModifierEncoderQuery: ModifierEncoder[QueryType]
    implicit def collectionModifierEncoderEntity: ModifierEncoder[EntityType]
    implicit def collectionUpdateQueryEncoder: UpdateQueryEncoder[EntityType]
    implicit def collectionUpsertEncoder: UpsertEncoder[EntityType]
}

private case class CollectionCodecSetImpl[QueryType, EntityType, IdType, ValueType](
    override val collectionValueDecoder: ValueDecoder[ValueType],
    override val collectionQueryEncoder: QueryEncoder[QueryType],
    override val collectionIdEncoder: IdEncoder[IdType],
    override val collectionQueryResultDecoder: QueryResultDecoder[EntityType],
    override val collectionModifierEncoderQuery: ModifierEncoder[QueryType],
    override val collectionModifierEncoderEntity: ModifierEncoder[EntityType],
    override val collectionUpdateQueryEncoder: UpdateQueryEncoder[EntityType],
    override val collectionUpsertEncoder: UpsertEncoder[EntityType])
    extends CollectionCodecSet[QueryType, EntityType, IdType, ValueType]

object CollectionCodecSet {
    def apply[QueryType, EntityType, IdType, ValueType]()(implicit collectionValueDecoder: ValueDecoder[ValueType],
        collectionQueryEncoder: QueryEncoder[QueryType],
        collectionIdEncoder: IdEncoder[IdType],
        collectionQueryResultDecoder: QueryResultDecoder[EntityType],
        collectionModifierEncoderQuery: ModifierEncoder[QueryType],
        collectionModifierEncoderEntity: ModifierEncoder[EntityType],
        collectionUpdateQueryEncoder: UpdateQueryEncoder[EntityType],
        collectionUpsertEncoder: UpsertEncoder[EntityType]): CollectionCodecSet[QueryType, EntityType, IdType, ValueType] =
        CollectionCodecSetImpl[QueryType, EntityType, IdType, ValueType](collectionValueDecoder,
            collectionQueryEncoder, collectionIdEncoder, collectionQueryResultDecoder, collectionModifierEncoderQuery,
            collectionModifierEncoderEntity, collectionUpdateQueryEncoder, collectionUpsertEncoder)
}
