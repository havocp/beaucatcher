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

    protected def copy[Q, E, D, I, V](collectionValueDecoder: ValueDecoder[V],
        collectionQueryEncoder: QueryEncoder[Q],
        collectionIdEncoder: IdEncoder[I],
        collectionQueryResultDecoder: QueryResultDecoder[D],
        collectionModifierEncoderQuery: ModifierEncoder[Q],
        collectionModifierEncoderEntity: ModifierEncoder[E],
        collectionUpdateQueryEncoder: UpdateQueryEncoder[E],
        collectionUpsertEncoder: UpsertEncoder[E]): CollectionCodecSet[Q, E, D, I, V] =
        CollectionCodecSetImpl[Q, E, D, I, V](collectionValueDecoder, collectionQueryEncoder,
            collectionIdEncoder, collectionQueryResultDecoder, collectionModifierEncoderQuery,
            collectionModifierEncoderEntity, collectionUpdateQueryEncoder,
            collectionUpsertEncoder)

    def toErrorIfResultDecoded: CollectionCodecSet[QueryType, EncodeEntityType, ErrorIfDecodedDocument, IdType, ValueType] =
        copy(collectionValueDecoder, collectionQueryEncoder, collectionIdEncoder,
            ErrorIfDecodedDocument.queryResultDecoder,
            collectionModifierEncoderQuery, collectionModifierEncoderEntity, collectionUpdateQueryEncoder, collectionUpsertEncoder)
    def toErrorIfValueDecoded: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ErrorIfDecodedValue] =
        copy(ErrorIfDecodedValue.valueDecoder, collectionQueryEncoder, collectionIdEncoder,
            collectionQueryResultDecoder,
            collectionModifierEncoderQuery, collectionModifierEncoderEntity, collectionUpdateQueryEncoder, collectionUpsertEncoder)
    def toErrorIfResultOrValueDecoded: CollectionCodecSet[QueryType, EncodeEntityType, ErrorIfDecodedDocument, IdType, ErrorIfDecodedValue] =
        copy(ErrorIfDecodedValue.valueDecoder, collectionQueryEncoder, collectionIdEncoder,
            ErrorIfDecodedDocument.queryResultDecoder,
            collectionModifierEncoderQuery, collectionModifierEncoderEntity, collectionUpdateQueryEncoder, collectionUpsertEncoder)
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
    extends CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] {

}

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

/** Abstract mixin that provides an ID encoder to a codec set. */
trait CollectionCodecSetIdEncoder[-IdType] {
    self: CollectionCodecSet[_, _, _, IdType, _] =>

    override implicit def collectionIdEncoder: IdEncoder[IdType]
}

/** Abstract mixin that provides query encoders to a codec set. */
trait CollectionCodecSetQueryEncoders[-QueryType] {
    self: CollectionCodecSet[QueryType, _, _, _, _] =>
    override implicit def collectionQueryEncoder: QueryEncoder[QueryType]
    override implicit def collectionModifierEncoderQuery: ModifierEncoder[QueryType]
}

/** Abstract mixin that provides entity encoders and decoders to a codec set */
trait CollectionCodecSetEntityCodecs[EntityType] {
    self: CollectionCodecSet[_, EntityType, EntityType, _, _] =>

    override implicit def collectionQueryResultDecoder: QueryResultDecoder[EntityType]
    override implicit def collectionModifierEncoderEntity: ModifierEncoder[EntityType]
    override implicit def collectionUpdateQueryEncoder: UpdateQueryEncoder[EntityType]
    override implicit def collectionUpsertEncoder: UpsertEncoder[EntityType]
}

/** Abstract mixin that provides a value decoder to a codec set */
trait CollectionCodecSetValueDecoder[+ValueType] {
    self: CollectionCodecSet[_, _, _, _, ValueType] =>

    override implicit def collectionValueDecoder: ValueDecoder[ValueType]
}
