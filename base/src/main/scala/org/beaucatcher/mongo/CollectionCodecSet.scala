package org.beaucatcher.mongo

import scala.annotation.implicitNotFound

/** The set of encoders and decoders required to read from a collection */
@implicitNotFound("Can't find a read-only codec set using ${QueryType} for queries, ${EntityType} for results, ${IdType} for IDs, and ${ValueType} for distinct values")
trait ReadOnlyCollectionCodecSet[-QueryType, +EntityType, -IdType, +ValueType] {
    implicit def collectionValueDecoder: ValueDecoder[ValueType]
    implicit def collectionQueryEncoder: QueryEncoder[QueryType]
    implicit def collectionIdEncoder: IdEncoder[IdType]
    implicit def collectionQueryResultDecoder: QueryResultDecoder[EntityType]
}

private case class ReadOnlyCollectionCodecSetImpl[-QueryType, +EntityType, -IdType, +ValueType](
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

/**
 * The set of encoders and decoders required to read and write a collection.
 * EncodeEntityType and DecodeEntityType are separate so they can be contravariant and covariant.
 */
@implicitNotFound("Can't find a codec set using ${QueryType} for queries, ${EncodeEntityType} for updates, ${DecodeEntityType} for results, ${IdType} for IDs, and ${ValueType} for distinct values")
trait CollectionCodecSet[-QueryType, -EncodeEntityType, +DecodeEntityType, -IdType, +ValueType]
    extends ReadOnlyCollectionCodecSet[QueryType, DecodeEntityType, IdType, ValueType] {

    implicit def collectionModifierEncoderQuery: ModifierEncoder[QueryType]
    implicit def collectionModifierEncoderEntity: ModifierEncoder[EncodeEntityType]
    implicit def collectionUpdateQueryEncoder: UpdateQueryEncoder[EncodeEntityType]
    implicit def collectionUpsertEncoder: UpsertEncoder[EncodeEntityType]
}

private case class CollectionCodecSetImpl[-QueryType, -EncodeEntityType, +DecodeEntityType, -IdType, +ValueType](
    override val collectionValueDecoder: ValueDecoder[ValueType],
    override val collectionQueryEncoder: QueryEncoder[QueryType],
    override val collectionIdEncoder: IdEncoder[IdType],
    override val collectionQueryResultDecoder: QueryResultDecoder[DecodeEntityType],
    override val collectionModifierEncoderQuery: ModifierEncoder[QueryType],
    override val collectionModifierEncoderEntity: ModifierEncoder[EncodeEntityType],
    override val collectionUpdateQueryEncoder: UpdateQueryEncoder[EncodeEntityType],
    override val collectionUpsertEncoder: UpsertEncoder[EncodeEntityType])
    extends CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]

object CollectionCodecSet {
    def apply[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]()(implicit collectionValueDecoder: ValueDecoder[ValueType],
        collectionQueryEncoder: QueryEncoder[QueryType],
        collectionIdEncoder: IdEncoder[IdType],
        collectionQueryResultDecoder: QueryResultDecoder[DecodeEntityType],
        collectionModifierEncoderQuery: ModifierEncoder[QueryType],
        collectionModifierEncoderEntity: ModifierEncoder[EncodeEntityType],
        collectionUpdateQueryEncoder: UpdateQueryEncoder[EncodeEntityType],
        collectionUpsertEncoder: UpsertEncoder[EncodeEntityType]): CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] =
        CollectionCodecSetImpl[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](collectionValueDecoder,
            collectionQueryEncoder, collectionIdEncoder, collectionQueryResultDecoder, collectionModifierEncoderQuery,
            collectionModifierEncoderEntity, collectionUpdateQueryEncoder, collectionUpsertEncoder)
}
