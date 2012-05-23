package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.driver._

/**
 * For more detail, please see [[org.beaucatcher.mongo.ReadOnlyAsyncCollection]],
 * which is the same except it returns the results wrapped in `Future`.
 */
sealed trait ReadOnlySyncCollection extends ReadOnlyCollection {

    protected[mongo] override def underlying: ReadOnlySyncDriverCollection

    final def count(): Long =
        underlying.count(EmptyDocument, CountOptions.empty)
    final def count[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): Long =
        count(query, CountOptions.empty)
    final def count[Q](query: Q, fields: Fields)(implicit queryEncoder: QueryEncoder[Q]): Long =
        count(query, CountOptions(fields = fields.toOption))
    final def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Long =
        underlying.count(query, options)

    final def distinct[V](key: String)(implicit valueDecoder: ValueDecoder[V]): Iterator[V] = {
        // we just need any query encoder to _not_ use to encode the None in DistinctOptions.empty
        import EmptyDocument.queryEncoder
        distinct(key, DistinctOptions.empty)
    }
    final def distinct[Q, V](key: String, query: Q)(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Iterator[V] =
        distinct(key, DistinctOptions[Q](query = Some(query)))
    final def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Iterator[V] =
        underlying.distinct(key, options)

    final def find[E]()(implicit resultDecoder: QueryResultDecoder[E]): Cursor[E] =
        underlying.find(EmptyDocument, FindOptions.empty)
    final def find[Q, E](query: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Cursor[E] =
        find(query: Q, FindOptions.empty)
    final def find[Q, E](query: Q, fields: Fields)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Cursor[E] =
        find(query: Q, FindOptions(fields = fields.toOption))
    final def find[Q, E](query: Q, fields: Fields, skip: Long, limit: Long, batchSize: Int)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Cursor[E] =
        find(query: Q, FindOptions(fields = fields.toOption, skip = Some(skip), limit = Some(limit), batchSize = Some(batchSize)))
    final def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Cursor[E] =
        underlying.find(query, options)

    final def findOne[E]()(implicit resultDecoder: QueryResultDecoder[E]): Option[E] =
        underlying.findOne(EmptyDocument, FindOneOptions.empty)
    final def findOne[Q, E](query: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findOne(query: Q, FindOneOptions.empty)
    final def findOne[Q, E](query: Q, fields: Fields)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findOne(query: Q, FindOneOptions(fields = fields.toOption))
    final def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        underlying.findOne(query, options)

    final def findOneById[I, E](id: I)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById[I, E](id: I, fields: Fields)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findOneById(id, FindOneByIdOptions(fields = fields.toOption))
    final def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Option[E] =
        underlying.findOneById(id, options)

    /**
     * Queries mongod for the indexes on this collection.
     */
    final def findIndexes(): Cursor[CollectionIndex] = {
        import Implicits._
        database.system.indexes.sync[CollectionIndex].find(BObject("ns" -> fullName))
    }
}

/**
 * For more detail, please see [[org.beaucatcher.mongo.BoundReadOnlyAsyncCollection]],
 * which is the same except it returns the results wrapped in `Future`.
 */
sealed trait BoundReadOnlySyncCollection[-QueryType, +DecodeEntityType, -IdType, +ValueType]
    extends BoundReadOnlyCollection[QueryType, DecodeEntityType, IdType, ValueType] {

    protected[mongo] override def unbound: ReadOnlySyncCollection

    // change the def to a val so we can import it
    protected[mongo] override val codecs: ReadOnlyCollectionCodecSet[QueryType, DecodeEntityType, IdType, ValueType]

    import codecs._

    final def count(): Long =
        unbound.count()
    final def count(query: QueryType): Long =
        unbound.count(query)
    final def count(query: QueryType, fields: Fields): Long =
        unbound.count(query, fields)
    final def count(query: QueryType, options: CountOptions): Long =
        unbound.count(query, options)
    final def distinct(key: String): Iterator[ValueType] =
        unbound.distinct(key)
    final def distinct(key: String, query: QueryType): Iterator[ValueType] =
        unbound.distinct(key, query)
    final def distinct(key: String, options: DistinctOptions[QueryType]): Iterator[ValueType] =
        unbound.distinct(key, options)
    final def find(): Cursor[DecodeEntityType] =
        unbound.find()
    final def find(query: QueryType): Cursor[DecodeEntityType] =
        unbound.find(query: QueryType)
    final def find(query: QueryType, fields: Fields): Cursor[DecodeEntityType] =
        unbound.find(query: QueryType, fields)
    final def find(query: QueryType, fields: Fields, skip: Long, limit: Long, batchSize: Int): Cursor[DecodeEntityType] =
        unbound.find(query: QueryType, fields, skip, limit, batchSize)
    final def find(query: QueryType, options: FindOptions): Cursor[DecodeEntityType] =
        unbound.find(query, options)
    final def findOne(): Option[DecodeEntityType] =
        unbound.findOne()
    final def findOne(query: QueryType): Option[DecodeEntityType] =
        unbound.findOne(query: QueryType)
    final def findOne(query: QueryType, fields: Fields): Option[DecodeEntityType] =
        unbound.findOne(query: QueryType, fields)
    final def findOne(query: QueryType, options: FindOneOptions): Option[DecodeEntityType] =
        unbound.findOne(query, options)
    final def findOneById(id: IdType): Option[DecodeEntityType] =
        unbound.findOneById(id)
    final def findOneById(id: IdType, fields: Fields): Option[DecodeEntityType] =
        unbound.findOneById(id, fields)
    final def findOneById(id: IdType, options: FindOneByIdOptions): Option[DecodeEntityType] =
        unbound.findOneById(id, options)
    final def findIndexes(): Cursor[CollectionIndex] =
        unbound.findIndexes()
}

/**
 * For more detail, please see [[org.beaucatcher.mongo.AsyncCollection]],
 * which is the same except it returns the results wrapped in `Future`.
 */
sealed trait SyncCollection
    extends ReadOnlySyncCollection with Collection {

    protected[mongo] override def underlying: SyncDriverCollection

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[Q, E](query: Q, update: Option[Q], options: FindAndModifyOptions[Q])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        underlying.findAndModify(query, update, options)

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     *
     * @param query query to find the object
     * @param o object with new values
     * @return old object
     */
    final def findAndReplace[Q, E, D](query: Q, o: E)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Option[D] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions.empty)
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Option[D] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](flags = flags))
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, sort: Q)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Option[D] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](sort = Some(sort)))
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, sort: Q, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Option[D] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](sort = Some(sort), flags = flags))
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, sort: Q, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Option[D] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](sort = Some(sort), fields = fields.toOption, flags = flags))

    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        underlying.findAndModify(query: Q, Some(modifier), FindAndModifyOptions.empty)

    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](flags = flags))
    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, sort: Q)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](sort = Some(sort)))
    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, sort: Q, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](sort = Some(sort), flags = flags))
    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, sort: Q, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](sort = Some(sort), fields = fields.toOption, flags = flags))
    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[Q, E](query: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] = {
        implicit val modifierEncoder: ModifierEncoder[Q] = AssertNotUsedEncoder
        findAndModify(query, None, FindAndModifyOptions.remove)
    }
    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[Q, E](query: Q, sort: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] = {
        implicit val modifierEncoder: ModifierEncoder[Q] = AssertNotUsedEncoder
        findAndModify(query, None, FindAndModifyOptions[Q](sort = Some(sort), flags = Set(FindAndModifyRemove)))
    }

    /**
     * Adds a new object to the collection. It's an error if an object with the same ID already exists.
     */
    final def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): WriteResult =
        underlying.insert(o)

    /**
     * Does an updateUpsert() on the object.
     *
     * Unlike save() in Casbah, does not look at mutable isNew() flag
     * in the ObjectId (if there's an ObjectId) so it never does an insert().
     * This is because our ObjectId is immutable. If you just created the
     * object, use insert() instead of save().
     */
    final def save[E](o: E)(implicit queryEncoder: UpdateQueryEncoder[E], upsertEncoder: UpsertEncoder[E]): WriteResult =
        underlying.save(o, UpdateOptions.upsert)

    /**
     * $findAndModifyVsUpdate
     */
    final def update[Q, M](query: Q, o: M)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): WriteResult =
        underlying.update(query, o, UpdateOptions.empty)

    /**
     * $findAndModifyVsUpdate
     */
    final def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): WriteResult =
        underlying.update(query, modifier, options)

    /**
     * $findAndModifyVsUpdate
     */
    final def updateUpsert[Q, E](query: Q, o: E)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[E]): WriteResult =
        underlying.updateUpsert(query, o, UpdateOptions.upsert)

    /**
     * Note that updating with the [[org.beaucatcher.mongo.UpdateMulti]] flag only works if you
     * do a "dollar sign operator," the modifier object can't just specify new field values or a
     * replacement object. This is a MongoDB thing, not a library/driver thing.
     *
     * $findAndModifyVsUpdate
     */
    final def updateMulti[Q, M](query: Q, modifier: M)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): WriteResult =
        update(query, modifier, UpdateOptions.multi)

    /**
     * Deletes all objects matching the query.
     */
    final def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): WriteResult =
        underlying.remove(query)

    /**
     * Deletes ALL objects in the collection. DANGER.
     */
    final def removeAll(): WriteResult =
        underlying.remove(EmptyDocument)
    /**
     * Deletes the object with the given ID, if any.
     */
    final def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): WriteResult =
        underlying.removeById(id)

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex[Q](keys: Q)(implicit queryEncoder: QueryEncoder[Q]): WriteResult =
        ensureIndex(keys, IndexOptions.empty)

    /**
     * Creates the given index on the collection, using custom options.
     */
    final def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): WriteResult =
        underlying.ensureIndex(keys, options)

    /**
     * Drops ALL indexes on the collection.
     *
     */
    final def dropIndexes(): CommandResult = dropIndex("*")

    /**
     * Removes the given index from the collection.
     */
    final def dropIndex(name: String): CommandResult =
        underlying.dropIndex(name)
}

/**
 * For more detail, please see [[org.beaucatcher.mongo.BoundAsyncCollection]],
 * which is the same except it returns the results wrapped in `Future`.
 */
sealed trait BoundSyncCollection[-QueryType, -EncodeEntityType, +DecodeEntityType, -IdType, +ValueType]
    extends BoundReadOnlySyncCollection[QueryType, DecodeEntityType, IdType, ValueType]
    with BoundCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] {

    protected[mongo] override def unbound: SyncCollection

    // change the def to a val so we can import it
    protected[mongo] override val codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]

    import codecs._

    final def findAndModify(query: QueryType, update: Option[QueryType], options: FindAndModifyOptions[QueryType]): Option[DecodeEntityType] =
        unbound.findAndModify(query, update, options)
    final def findAndReplace(query: QueryType, o: EncodeEntityType): Option[DecodeEntityType] =
        unbound.findAndReplace(query, o)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, flags: Set[FindAndModifyFlag]): Option[DecodeEntityType] =
        unbound.findAndReplace(query, o, flags)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, sort: QueryType): Option[DecodeEntityType] =
        unbound.findAndReplace(query, o, sort)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, sort: QueryType, flags: Set[FindAndModifyFlag]): Option[DecodeEntityType] =
        unbound.findAndReplace(query, o, sort, flags)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, sort: QueryType, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Option[DecodeEntityType] =
        unbound.findAndReplace(query, o, sort, fields, flags)
    final def findAndModify(query: QueryType, modifier: QueryType): Option[DecodeEntityType] =
        unbound.findAndModify(query, modifier)
    final def findAndModify(query: QueryType, modifier: QueryType, flags: Set[FindAndModifyFlag]): Option[DecodeEntityType] =
        unbound.findAndModify(query, modifier, flags)
    final def findAndModify(query: QueryType, modifier: QueryType, sort: QueryType): Option[DecodeEntityType] =
        unbound.findAndModify(query, modifier, sort)
    final def findAndModify(query: QueryType, modifier: QueryType, sort: QueryType, flags: Set[FindAndModifyFlag]): Option[DecodeEntityType] =
        unbound.findAndModify(query, modifier, sort, flags)
    final def findAndModify(query: QueryType, modifier: QueryType, sort: QueryType, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Option[DecodeEntityType] =
        unbound.findAndModify(query, modifier, sort, fields, flags)
    final def findAndRemove(query: QueryType): Option[DecodeEntityType] =
        unbound.findAndRemove(query)
    final def findAndRemove(query: QueryType, sort: QueryType): Option[DecodeEntityType] =
        unbound.findAndRemove(query, sort)
    final def insert(o: EncodeEntityType): WriteResult =
        unbound.insert(o)
    final def save(o: EncodeEntityType): WriteResult =
        unbound.save(o)
    // in the unbound object, there's only one update(), here we split it in two
    // based on the entity vs. query modifier
    final def update(query: QueryType, o: EncodeEntityType): WriteResult =
        unbound.update(query, o)
    final def updateWithModifier(query: QueryType, modifier: QueryType): WriteResult =
        unbound.update(query, modifier)
    final def update(query: QueryType, o: EncodeEntityType, options: UpdateOptions): WriteResult =
        unbound.update(query, o, options)
    final def updateWithModifier(query: QueryType, modifier: QueryType, options: UpdateOptions): WriteResult =
        unbound.update(query, modifier, options)
    final def updateUpsert(query: QueryType, o: EncodeEntityType): WriteResult =
        unbound.updateUpsert(query, o)
    final def updateMulti(query: QueryType, modifier: QueryType): WriteResult =
        unbound.updateMulti(query, modifier)

    final def remove(query: QueryType): WriteResult =
        unbound.remove(query)
    final def removeAll(): WriteResult =
        unbound.removeAll()
    final def removeById(id: IdType): WriteResult =
        unbound.removeById(id)

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex(keys: QueryType): WriteResult =
        unbound.ensureIndex(keys)
    /**
     * Creates the given index on the collection, using custom options.
     */
    final def ensureIndex(keys: QueryType, options: IndexOptions): WriteResult =
        unbound.ensureIndex(keys, options)

    final def dropIndexes(): CommandResult =
        unbound.dropIndexes()

    /**
     * Removes the given index from the collection.
     */
    final def dropIndex(name: String): CommandResult =
        unbound.dropIndex(name)
}

object SyncCollection {

    private final class SyncCollectionImpl(name: String, override val context: Context)
        extends SyncCollection {
        override val underlying = context.driver.newSyncCollection(name)(context.driverContext)
    }

    def apply(name: String)(implicit context: Context): SyncCollection = {
        new SyncCollectionImpl(name, context)
    }
}

object BoundSyncCollection {

    private final class BoundSyncCollectionImpl[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](override val unbound: SyncCollection)(implicit override val codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType])
        extends BoundSyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] {
    }

    def apply[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](name: String)(implicit context: Context, codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]): BoundSyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] = {
        new BoundSyncCollectionImpl(SyncCollection(name))
    }

    def apply[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](unbound: SyncCollection)(implicit codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]): BoundSyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] = {
        new BoundSyncCollectionImpl(unbound)
    }

    // implicitly convert to BoundSyncCollection from SyncCollection
    implicit def bind[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](unbound: SyncCollection)(implicit codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]): BoundSyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] = {
        apply(unbound)
    }
}
