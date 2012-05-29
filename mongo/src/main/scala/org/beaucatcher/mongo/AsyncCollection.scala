package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.driver._
import akka.dispatch.Future

/**
 * Trait expressing all read operations on a collection in asynchronous form,
 * with wire encoding and decoding delegated to implicit typeclass parameters.
 * For write operations, see [[org.beaucatcher.mongo.AsyncCollection]].
 * To avoid the use of implicits, see [[org.beaucatcher.mongo.BoundReadOnlyAsyncCollection]].
 * There's no way to create a read-only collection directly, the idea is that you
 * can create a read-write collection and then pass it around with the read-only type
 * to any code you want to be read-only.
 */
sealed trait ReadOnlyAsyncCollection extends ReadOnlyCollection {

    protected[mongo] override def underlying: ReadOnlyAsyncDriverCollection

    final def count(): Future[Long] =
        underlying.count(EmptyDocument, CountOptions.empty)
    final def count[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): Future[Long] =
        count(query, CountOptions.empty)
    final def count[Q](query: Q, fields: Fields)(implicit queryEncoder: QueryEncoder[Q]): Future[Long] =
        count(query, CountOptions(fields = fields.toOption))
    final def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[Long] =
        underlying.count(query, options)

    final def distinct[V](key: String)(implicit valueDecoder: ValueDecoder[V]): Future[Iterator[V]] = {
        // we just need any query encoder to _not_ use to encode the None in DistinctOptions.empty
        import EmptyDocument.queryEncoder
        distinct(key, DistinctOptions.empty)
    }
    final def distinct[Q, V](key: String, query: Q)(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Future[Iterator[V]] =
        distinct(key, DistinctOptions[Q](query = Some(query)))
    final def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Future[Iterator[V]] =
        underlying.distinct(key, options)

    final def find[E]()(implicit resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] =
        underlying.find(EmptyDocument, FindOptions.empty)
    final def find[Q, E](query: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] =
        find(query: Q, FindOptions.empty)
    final def find[Q, E](query: Q, fields: Fields)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] =
        find(query: Q, FindOptions(fields = fields.toOption))
    final def find[Q, E](query: Q, fields: Fields, skip: Long, limit: Long, batchSize: Int)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] =
        find(query: Q, FindOptions(fields = fields.toOption, skip = Some(skip), limit = Some(limit), batchSize = Some(batchSize)))
    final def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] =
        underlying.find(query, options)

    final def findOne[E]()(implicit resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        underlying.findOne(EmptyDocument, FindOneOptions.empty)
    final def findOne[Q, E](query: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findOne(query: Q, FindOneOptions.empty)
    final def findOne[Q, E](query: Q, fields: Fields)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findOne(query: Q, FindOneOptions(fields = fields.toOption))
    final def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        underlying.findOne(query, options)

    final def findOneById[I, E](id: I)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById[I, E](id: I, fields: Fields)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findOneById(id, FindOneByIdOptions(fields = fields.toOption))
    final def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        underlying.findOneById(id, options)

    /**
     * Queries mongod for the indexes on this collection.
     */
    final def findIndexes(): Future[AsyncCursor[CollectionIndex]] = {
        database.system.indexes.async[CollectionIndex].find(Iterator("ns" -> fullName))
    }
}

/**
 * Version of [[org.beaucatcher.mongo.ReadOnlyAsyncCollection]] using predetermined parameter types
 * rather than any parameter types with the appropriate implicit encoders and decoders available.
 * For write operations, see [[org.beaucatcher.mongo.BoundAsyncCollection]].
 * There's no way to create a read-only collection directly, the idea is that you
 * can create a read-write collection and then pass it around with the read-only type
 * to any code you want to be read-only.
 */
sealed trait BoundReadOnlyAsyncCollection[-QueryType, +DecodeEntityType, -IdType, +ValueType]
    extends BoundReadOnlyCollection[QueryType, DecodeEntityType, IdType, ValueType] {

    protected[mongo] override def unbound: ReadOnlyAsyncCollection

    // change the def to a val so we can import it
    protected[mongo] override val codecs: ReadOnlyCollectionCodecSet[QueryType, DecodeEntityType, IdType, ValueType]

    import codecs._

    final def count(): Future[Long] =
        unbound.count()
    final def count(query: QueryType): Future[Long] =
        unbound.count(query)
    final def count(query: QueryType, fields: Fields): Future[Long] =
        unbound.count(query, fields)
    final def count(query: QueryType, options: CountOptions): Future[Long] =
        unbound.count(query, options)
    final def distinct(key: String): Future[Iterator[ValueType]] =
        unbound.distinct(key)
    final def distinct(key: String, query: QueryType): Future[Iterator[ValueType]] =
        unbound.distinct(key, query)
    final def distinct(key: String, options: DistinctOptions[QueryType]): Future[Iterator[ValueType]] =
        unbound.distinct(key, options)
    final def find(): Future[AsyncCursor[DecodeEntityType]] =
        unbound.find()
    final def find(query: QueryType): Future[AsyncCursor[DecodeEntityType]] =
        unbound.find(query: QueryType)
    final def find(query: QueryType, fields: Fields): Future[AsyncCursor[DecodeEntityType]] =
        unbound.find(query: QueryType, fields)
    final def find(query: QueryType, fields: Fields, skip: Long, limit: Long, batchSize: Int): Future[AsyncCursor[DecodeEntityType]] =
        unbound.find(query: QueryType, fields, skip, limit, batchSize)
    final def find(query: QueryType, options: FindOptions): Future[AsyncCursor[DecodeEntityType]] =
        unbound.find(query, options)
    final def findOne(): Future[Option[DecodeEntityType]] =
        unbound.findOne()
    final def findOne(query: QueryType): Future[Option[DecodeEntityType]] =
        unbound.findOne(query: QueryType)
    final def findOne(query: QueryType, fields: Fields): Future[Option[DecodeEntityType]] =
        unbound.findOne(query: QueryType, fields)
    final def findOne(query: QueryType, options: FindOneOptions): Future[Option[DecodeEntityType]] =
        unbound.findOne(query, options)
    final def findOneById(id: IdType): Future[Option[DecodeEntityType]] =
        unbound.findOneById(id)
    final def findOneById(id: IdType, fields: Fields): Future[Option[DecodeEntityType]] =
        unbound.findOneById(id, fields)
    final def findOneById(id: IdType, options: FindOneByIdOptions): Future[Option[DecodeEntityType]] =
        unbound.findOneById(id, options)
    final def findIndexes(): Future[AsyncCursor[CollectionIndex]] =
        unbound.findIndexes()
}

/**
 * Trait containing all read and write operations on a collection, in asynchronous form,
 * with wire encoding and decoding delegated to implicit typeclass parameters.
 * For the synchronous equivalent, see [[org.beaucatcher.mongo.SyncCollection]].
 * To avoid the use of implicits, see [[org.beaucatcher.mongo.BoundAsyncCollection]].
 *
 * The recommended way to obtain an instance of [[org.beaucatcher.mongo.AsyncCollection]]
 * is from the `async` property on [[org.beaucatcher.mongo.CollectionAccess]],
 * which would in turn be implemented by the companion object of a case class
 * representing an object in a collection. For example you might have `case class Foo`
 * representing objects in the `foo` collection, with a companion object `object Foo`
 * which implements [[org.beaucatcher.mongo.CollectionAccess]]. You would then
 * write code such as:
 * {{{
 *    Foo.async[BObject].find() // obtain results as a BObject
 *    Foo.async[Foo].find()     // obtain results as a case class instance
 *    Foo.async.count()         // entity type not relevant
 * }}}
 * This is only a convention though, of course there's more than one way to do it.
 *
 * @define findAndModifyVsUpdate
 *
 *   findAndModify(), findAndReplace() will affect only one object and will return
 *   either the old or the new object depending on whether you specify the
 *   [[org.beaucatcher.mongo.FindAndModifyNew]] flag. update() and its variants
 *   do not return an object, and with updateMulti() you can modify multiple
 *   objects at once.
 *
 * @define findAndReplaceDocs
 *
 *   Finds the first object matching the query and updates it with the
 *   values from the provided entity. However, the ID is not updated.
 *   Returns the old object, or the new one if the
 *   [[org.beaucatcher.mongo.FindAndModifyNew]] flag was provided.
 *   If multiple objects match the query, then the `sort` parameter
 *   determines which will be replaced; only one object is ever replaced.
 *   A sort object is a map from fields to the integer "1" for ascending,
 *   "-1" for descending.
 *   If specified, the `fields` parameter determines which fields are
 *   returned in the old (or new) object returned from the method.
 */
sealed trait AsyncCollection
    extends ReadOnlyAsyncCollection with Collection {

    protected[mongo] override def underlying: AsyncDriverCollection

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[Q, E](query: Q, update: Option[Q], options: FindAndModifyOptions[Q])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
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
    final def findAndReplace[Q, E, D](query: Q, o: E)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Future[Option[D]] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions.empty)
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Future[Option[D]] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](flags = flags))
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, sort: Q)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Future[Option[D]] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](sort = Some(sort)))
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, sort: Q, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Future[Option[D]] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](sort = Some(sort), flags = flags))
    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[Q, E, D](query: Q, o: E, sort: Q, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[E], resultDecoder: QueryResultDecoder[D]): Future[Option[D]] =
        underlying.findAndModify(query: Q, Some(o), FindAndModifyOptions[Q](sort = Some(sort), fields = fields.toOption, flags = flags))

    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        underlying.findAndModify(query: Q, Some(modifier), FindAndModifyOptions.empty)

    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](flags = flags))
    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, sort: Q)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](sort = Some(sort)))
    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, sort: Q, flags: Set[FindAndModifyFlag])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](sort = Some(sort), flags = flags))
    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[Q, E](query: Q, modifier: Q, sort: Q, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        findAndModify(query, Some(modifier),
            FindAndModifyOptions[Q](sort = Some(sort), fields = fields.toOption, flags = flags))
    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[Q, E](query: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] = {
        implicit val modifierEncoder: ModifierEncoder[Q] = AssertNotUsedEncoder
        findAndModify(query, None, FindAndModifyOptions.remove)
    }
    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[Q, E](query: Q, sort: Q)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] = {
        implicit val modifierEncoder: ModifierEncoder[Q] = AssertNotUsedEncoder
        findAndModify(query, None, FindAndModifyOptions[Q](sort = Some(sort), flags = Set(FindAndModifyRemove)))
    }

    /**
     * Adds a new object to the collection. It's an error if an object with the same ID already exists.
     */
    final def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): Future[WriteResult] =
        underlying.insert(o)

    /**
     * Does an updateUpsert() on the object.
     *
     * Unlike save() in Casbah, does not look at mutable isNew() flag
     * in the ObjectId (if there's an ObjectId) so it never does an insert().
     * This is because our ObjectId is immutable. If you just created the
     * object, use insert() instead of save().
     */
    final def save[E](o: E)(implicit queryEncoder: UpdateQueryEncoder[E], upsertEncoder: UpsertEncoder[E]): Future[WriteResult] =
        underlying.save(o, UpdateOptions.upsert)

    /**
     * $findAndModifyVsUpdate
     */
    final def update[Q, M](query: Q, o: M)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): Future[WriteResult] =
        underlying.update(query, o, UpdateOptions.empty)

    /**
     * $findAndModifyVsUpdate
     */
    final def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): Future[WriteResult] =
        underlying.update(query, modifier, options)

    /**
     * $findAndModifyVsUpdate
     */
    final def updateUpsert[Q, E](query: Q, o: E)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[E]): Future[WriteResult] =
        underlying.updateUpsert(query, o, UpdateOptions.upsert)

    /**
     * Note that updating with the [[org.beaucatcher.mongo.UpdateMulti]] flag only works if you
     * do a "dollar sign operator," the modifier object can't just specify new field values or a
     * replacement object. This is a MongoDB thing, not a library/driver thing.
     *
     * $findAndModifyVsUpdate
     */
    final def updateMulti[Q, M](query: Q, modifier: M)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): Future[WriteResult] =
        update(query, modifier, UpdateOptions.multi)

    /**
     * Deletes all objects matching the query.
     */
    final def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] =
        underlying.remove(query)

    /**
     * Deletes ALL objects in the collection. DANGER.
     */
    final def removeAll(): Future[WriteResult] =
        underlying.remove(EmptyDocument)
    /**
     * Deletes the object with the given ID, if any.
     */
    final def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): Future[WriteResult] =
        underlying.removeById(id)

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex[Q](keys: Q)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] =
        ensureIndex(keys, IndexOptions.empty)

    /**
     * Creates the given index on the collection, using custom options.
     */
    final def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] =
        underlying.ensureIndex(keys, options)

    /**
     * Drops ALL indexes on the collection.
     *
     */
    final def dropIndexes(): Future[CommandResult] = dropIndex("*")

    /**
     * Removes the given index from the collection.
     */
    final def dropIndex(name: String): Future[CommandResult] =
        underlying.dropIndex(name)
}

/**
 * Version of [[org.beaucatcher.mongo.AsyncCollection]] using predetermined parameter types
 * rather than any parameter types with the appropriate implicit encoders and decoders available.
 */
sealed trait BoundAsyncCollection[-QueryType, -EncodeEntityType, +DecodeEntityType, -IdType, +ValueType]
    extends BoundReadOnlyAsyncCollection[QueryType, DecodeEntityType, IdType, ValueType]
    with BoundCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] {

    protected[mongo] override def unbound: AsyncCollection

    // change the def to a val so we can import it
    protected[mongo] override val codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]

    import codecs._

    final def findAndModify(query: QueryType, update: Option[QueryType], options: FindAndModifyOptions[QueryType]): Future[Option[DecodeEntityType]] =
        unbound.findAndModify(query, update, options)
    final def findAndReplace(query: QueryType, o: EncodeEntityType): Future[Option[DecodeEntityType]] =
        unbound.findAndReplace(query, o)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, flags: Set[FindAndModifyFlag]): Future[Option[DecodeEntityType]] =
        unbound.findAndReplace(query, o, flags)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, sort: QueryType): Future[Option[DecodeEntityType]] =
        unbound.findAndReplace(query, o, sort)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, sort: QueryType, flags: Set[FindAndModifyFlag]): Future[Option[DecodeEntityType]] =
        unbound.findAndReplace(query, o, sort, flags)
    final def findAndReplace(query: QueryType, o: EncodeEntityType, sort: QueryType, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Future[Option[DecodeEntityType]] =
        unbound.findAndReplace(query, o, sort, fields, flags)
    final def findAndModify(query: QueryType, modifier: QueryType): Future[Option[DecodeEntityType]] =
        unbound.findAndModify(query, modifier)
    final def findAndModify(query: QueryType, modifier: QueryType, flags: Set[FindAndModifyFlag]): Future[Option[DecodeEntityType]] =
        unbound.findAndModify(query, modifier, flags)
    final def findAndModify(query: QueryType, modifier: QueryType, sort: QueryType): Future[Option[DecodeEntityType]] =
        unbound.findAndModify(query, modifier, sort)
    final def findAndModify(query: QueryType, modifier: QueryType, sort: QueryType, flags: Set[FindAndModifyFlag]): Future[Option[DecodeEntityType]] =
        unbound.findAndModify(query, modifier, sort, flags)
    final def findAndModify(query: QueryType, modifier: QueryType, sort: QueryType, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Future[Option[DecodeEntityType]] =
        unbound.findAndModify(query, modifier, sort, fields, flags)
    final def findAndRemove(query: QueryType): Future[Option[DecodeEntityType]] =
        unbound.findAndRemove(query)
    final def findAndRemove(query: QueryType, sort: QueryType): Future[Option[DecodeEntityType]] =
        unbound.findAndRemove(query, sort)
    final def insert(o: EncodeEntityType): Future[WriteResult] =
        unbound.insert(o)
    final def save(o: EncodeEntityType): Future[WriteResult] =
        unbound.save(o)
    // in the unbound object, there's only one update(), here we split it in two
    // based on the entity vs. query modifier
    final def update(query: QueryType, o: EncodeEntityType): Future[WriteResult] =
        unbound.update(query, o)
    final def updateWithModifier(query: QueryType, modifier: QueryType): Future[WriteResult] =
        unbound.update(query, modifier)
    final def update(query: QueryType, o: EncodeEntityType, options: UpdateOptions): Future[WriteResult] =
        unbound.update(query, o, options)
    final def updateWithModifier(query: QueryType, modifier: QueryType, options: UpdateOptions): Future[WriteResult] =
        unbound.update(query, modifier, options)
    final def updateUpsert(query: QueryType, o: EncodeEntityType): Future[WriteResult] =
        unbound.updateUpsert(query, o)
    final def updateMulti(query: QueryType, modifier: QueryType): Future[WriteResult] =
        unbound.updateMulti(query, modifier)

    final def remove(query: QueryType): Future[WriteResult] =
        unbound.remove(query)
    final def removeAll(): Future[WriteResult] =
        unbound.removeAll()
    final def removeById(id: IdType): Future[WriteResult] =
        unbound.removeById(id)

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex(keys: QueryType): Future[WriteResult] =
        unbound.ensureIndex(keys)
    /**
     * Creates the given index on the collection, using custom options.
     */
    final def ensureIndex(keys: QueryType, options: IndexOptions): Future[WriteResult] =
        unbound.ensureIndex(keys, options)

    final def dropIndexes(): Future[CommandResult] =
        unbound.dropIndexes()

    /**
     * Removes the given index from the collection.
     */
    final def dropIndex(name: String): Future[CommandResult] =
        unbound.dropIndex(name)
}

object AsyncCollection {

    private final class AsyncCollectionImpl(name: String, override val context: Context)
        extends AsyncCollection {
        override val underlying = context.driver.newAsyncCollection(name)(context.driverContext)
    }

    def apply(name: String)(implicit context: Context): AsyncCollection = {
        new AsyncCollectionImpl(name, context)
    }
}

object BoundAsyncCollection {

    private final class BoundAsyncCollectionImpl[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](override val unbound: AsyncCollection)(implicit override val codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType])
        extends BoundAsyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] {
    }

    def apply[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](name: String)(implicit context: Context, codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]): BoundAsyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] = {
        new BoundAsyncCollectionImpl(AsyncCollection(name))
    }

    def apply[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](unbound: AsyncCollection)(implicit codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]): BoundAsyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] = {
        new BoundAsyncCollectionImpl(unbound)
    }

    // implicitly convert to BoundAsyncCollection from AsyncCollection
    implicit def bind[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType](unbound: AsyncCollection)(implicit codecs: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]): BoundAsyncCollection[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType] = {
        apply(unbound)
    }
}
