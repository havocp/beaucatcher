package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.driver._

trait ReadOnlySyncCollection[QueryType, EntityType, IdType, ValueType]
    extends ReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {

    protected[mongo] override def underlying: ReadOnlySyncDriverCollection

    import codecs._

    final def count(): Long =
        underlying.count(EmptyDocument, CountOptions.empty)
    final def count[A <% QueryType](query: A): Long =
        count(query: QueryType, CountOptions.empty)
    final def count[A <% QueryType](query: A, fields: Fields): Long =
        count(query: QueryType, CountOptions(fields = fields.toOption))

    final def count(query: QueryType, options: CountOptions): Long =
        underlying.count(query, options)

    final def distinct(key: String): Iterator[ValueType] =
        distinct(key, DistinctOptions.empty)
    final def distinct[A <% QueryType](key: String, query: A): Iterator[ValueType] =
        distinct(key, DistinctOptions[QueryType](query = Some(query)))

    final def distinct(key: String, options: DistinctOptions[QueryType]): Iterator[ValueType] =
        underlying.distinct(key, options)

    final def find(): Cursor[EntityType] =
        underlying.find(EmptyDocument, FindOptions.empty)
    final def find[A <% QueryType](query: A): Cursor[EntityType] =
        find(query: QueryType, FindOptions.empty)
    final def find[A <% QueryType](query: A, fields: Fields): Cursor[EntityType] =
        find(query: QueryType, FindOptions(fields = fields.toOption))
    final def find[A <% QueryType](query: A, fields: Fields, skip: Long, limit: Long, batchSize: Int): Cursor[EntityType] =
        find(query: QueryType, FindOptions(fields = fields.toOption, skip = Some(skip), limit = Some(limit), batchSize = Some(batchSize)))

    final def find(query: QueryType, options: FindOptions): Cursor[EntityType] =
        underlying.find(query, options)

    final def findOne(): Option[EntityType] =
        underlying.findOne(EmptyDocument, FindOneOptions.empty)
    final def findOne[A <% QueryType](query: A): Option[EntityType] =
        findOne(query: QueryType, FindOneOptions.empty)
    final def findOne[A <% QueryType](query: A, fields: Fields): Option[EntityType] =
        findOne(query: QueryType, FindOneOptions(fields = fields.toOption))

    final def findOne(query: QueryType, options: FindOneOptions): Option[EntityType] =
        underlying.findOne(query, options)

    final def findOneById(id: IdType): Option[EntityType] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById(id: IdType, fields: Fields): Option[EntityType] =
        findOneById(id, FindOneByIdOptions(fields = fields.toOption))

    final def findOneById(id: IdType, options: FindOneByIdOptions): Option[EntityType] =
        underlying.findOneById(id, options)

    /**
     * Queries mongod for the indexes on this collection.
     */
    final def findIndexes(): Iterator[CollectionIndex] = {
        import Implicits._
        database.system.indexes.sync[CollectionIndex].find(BObject("ns" -> fullName))
    }
}

/**
 * Trait expressing all data-access operations on a collection in synchronous form.
 * This trait does not include setup and teardown operations such as creating or
 * removing indexes; you would use the underlying API such as Casbah or Hammersmith
 * for that, for now.
 *
 * The recommended way to obtain an instance of [[org.beaucatcher.mongo.SyncCollection]]
 * is from the `sync` property on [[org.beaucatcher.mongo.CollectionAccess]],
 * which would in turn be implemented by the companion object of a case class
 * representing an object in a collection. For example you might have `case class Foo`
 * representing objects in the `foo` collection, with a companion object `object Foo`
 * which implements [[org.beaucatcher.mongo.CollectionAccess]]. You would then
 * write code such as:
 * {{{
 *    Foo.sync[BObject].find() // obtain results as a BObject
 *    Foo.sync[Foo].find()     // obtain results as a case class instance
 *    Foo.sync.count()         // entity type not relevant
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
trait SyncCollection[QueryType, EntityType, IdType, ValueType]
    extends ReadOnlySyncCollection[QueryType, EntityType, IdType, ValueType]
    with Collection[QueryType, EntityType, IdType, ValueType] {

    protected[mongo] override def underlying: SyncDriverCollection

    import codecs._

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     *
     * @param query query to find the object
     * @param o object with new values
     * @return old object
     */
    final def findAndReplace[A <% QueryType](query: A, o: EntityType): Option[EntityType] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions.empty)

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[A <% QueryType](query: A, o: EntityType, flags: Set[FindAndModifyFlag]): Option[EntityType] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](flags = flags))

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[A <% QueryType, B <% QueryType](query: A, o: EntityType, sort: B): Option[EntityType] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](sort = Some(sort)))

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[A <% QueryType, B <% QueryType](query: A, o: EntityType, sort: B, flags: Set[FindAndModifyFlag]): Option[EntityType] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](sort = Some(sort), flags = flags))

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     *
     * @param query query to find the object
     * @param o object with new values
     * @param sort sort object
     * @param fields fields to include in returned object
     * @param flags may include [[org.beaucatcher.mongo.FindAndModifyNew]] to return new rather than old object
     * @return old or new object according to flags
     */
    final def findAndReplace[A <% QueryType, B <% QueryType](query: A, o: EntityType, sort: B, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Option[EntityType] =
        underlying.findAndModify(query: QueryType, Some(o),
            FindAndModifyOptions[QueryType](sort = Some(sort), fields = fields.toOption, flags = flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType](query: A, modifier: B): Option[EntityType] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions.empty)

    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[A <% QueryType, B <% QueryType](query: A, modifier: B, flags: Set[FindAndModifyFlag]): Option[EntityType] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](flags = flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query: A, modifier: B, sort: C): Option[EntityType] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort)))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query: A, modifier: B, sort: C, flags: Set[FindAndModifyFlag]): Option[EntityType] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort), flags = flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query: A, modifier: B, sort: C, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Option[EntityType] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort), fields = fields.toOption, flags = flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[A <% QueryType](query: QueryType): Option[EntityType] =
        findAndModify(query: QueryType, None, FindAndModifyOptions.remove)

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[A <% QueryType, B <% QueryType](query: A, sort: B): Option[EntityType] =
        findAndModify(query: QueryType, None, FindAndModifyOptions[QueryType](sort = Some(sort), flags = Set(FindAndModifyRemove)))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify(query: QueryType, update: Option[QueryType], options: FindAndModifyOptions[QueryType]): Option[EntityType] =
        underlying.findAndModify(query, update, options)

    /**
     * Adds a new object to the collection. It's an error if an object with the same ID already exists.
     */
    final def insert(o: EntityType): WriteResult =
        underlying.insert(o)

    /**
     * Does an updateUpsert() on the object.
     *
     * Unlike save() in Casbah, does not look at mutable isNew() flag
     * in the ObjectId if there's an ObjectId so it never does an insert().
     * I guess this may be less efficient, but ObjectId.isNew() seems like
     * a total hack to me. Would rather just require people to use insert()
     * if they just finished creating the object.
     */
    final def save(o: EntityType): WriteResult =
        underlying.save(o, UpdateOptions.upsert)

    /**
     * $findAndModifyVsUpdate
     */
    final def update[A <% QueryType](query: A, o: EntityType): WriteResult =
        underlying.update(query: QueryType, o, UpdateOptions.empty)
    /**
     * $findAndModifyVsUpdate
     */
    final def updateUpsert[A <% QueryType](query: A, o: EntityType): WriteResult =
        underlying.updateUpsert(query: QueryType, o, UpdateOptions.upsert)

    /* Note: multi updates are not allowed with a replacement object, only with
     * "dollar sign operator" modifier objects. So there is no updateMulti overload
     * taking an entity object.
     */

    /**
     * $findAndModifyVsUpdate
     */
    final def update[A <% QueryType, B <% QueryType](query: A, modifier: B): WriteResult =
        update(query: QueryType, modifier, UpdateOptions.empty)
    /**
     * $findAndModifyVsUpdate
     */
    final def updateUpsert[A <% QueryType, B <% QueryType](query: A, modifier: B): WriteResult =
        update(query: QueryType, modifier, UpdateOptions.upsert)
    /**
     * Note that updating with the [[org.beaucatcher.mongo.UpdateMulti]] flag only works if you
     * do a "dollar sign operator," the modifier object can't just specify new field values. This is a
     * MongoDB thing, not a library/driver thing.
     *
     * $findAndModifyVsUpdate
     */
    final def updateMulti[A <% QueryType, B <% QueryType](query: A, modifier: B): WriteResult =
        update(query: QueryType, modifier, UpdateOptions.multi)
    /**
     * $findAndModifyVsUpdate
     */
    final def update(query: QueryType, modifier: QueryType, options: UpdateOptions): WriteResult =
        underlying.update(query, modifier, options)

    /**
     * Deletes all objects matching the query.
     */
    final def remove(query: QueryType): WriteResult =
        underlying.remove(query)

    /**
     * Deletes ALL objects.
     */
    final def removeAll(): WriteResult = {
        underlying.remove(EmptyDocument)
    }

    /**
     * Deletes the object with the given ID, if any.
     */
    final def removeById(id: IdType): WriteResult =
        underlying.removeById(id)

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex(keys: QueryType): WriteResult =
        ensureIndex(keys, IndexOptions.empty)

    /**
     * Creates the given index on the collection, using custom options.
     */
    final def ensureIndex(keys: QueryType, options: IndexOptions): WriteResult =
        underlying.ensureIndex(keys, options)

    final def dropIndexes(): CommandResult = dropIndex("*")

    /**
     * Removes the given index from the collection.
     */
    final def dropIndex(name: String): CommandResult =
        underlying.dropIndex(name)
}

object SyncCollection {
    private class SyncCollectionImpl[QueryType, EntityType, IdType, ValueType](name: String, override val context: Context,
        override val codecs: CollectionCodecSet[QueryType, EntityType, IdType, ValueType])
        extends SyncCollection[QueryType, EntityType, IdType, ValueType] {
        override val underlying = context.driver.newSyncCollection(name)(context.driverContext)
    }

    def apply[QueryType, EntityType, IdType, ValueType](name: String, codecs: CollectionCodecSet[QueryType, EntityType, IdType, ValueType])(implicit context: Context): SyncCollection[QueryType, EntityType, IdType, ValueType] = {
        new SyncCollectionImpl(name, context, codecs)
    }
}
