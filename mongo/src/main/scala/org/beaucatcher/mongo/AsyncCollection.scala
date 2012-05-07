package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.driver._
import akka.dispatch.Future

trait ReadOnlyAsyncCollection[QueryType, EntityType, IdType, ValueType]
    extends ReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {

    protected[mongo] override def underlying: ReadOnlyAsyncDriverCollection

    import codecs._

    final def count(): Future[Long] =
        underlying.count(EmptyDocument, CountOptions.empty)
    final def count[A <% QueryType](query: A): Future[Long] =
        count(query: QueryType, CountOptions.empty)
    final def count[A <% QueryType](query: A, fields: Fields): Future[Long] =
        count(query: QueryType, CountOptions(fields = fields.toOption))

    final def count(query: QueryType, options: CountOptions): Future[Long] =
        underlying.count(query, options)

    final def distinct(key: String): Future[Iterator[ValueType]] =
        distinct(key, DistinctOptions.empty)
    final def distinct[A <% QueryType](key: String, query: A): Future[Iterator[ValueType]] =
        distinct(key, DistinctOptions[QueryType](query = Some(query)))

    final def distinct(key: String, options: DistinctOptions[QueryType]): Future[Iterator[ValueType]] =
        underlying.distinct(key, options)

    final def find(): Future[AsyncCursor[EntityType]] =
        underlying.find(EmptyDocument, FindOptions.empty)
    final def find[A <% QueryType](query: A): Future[AsyncCursor[EntityType]] =
        find(query: QueryType, FindOptions.empty)
    final def find[A <% QueryType](query: A, fields: Fields): Future[AsyncCursor[EntityType]] =
        find(query: QueryType, FindOptions(fields = fields.toOption))
    final def find[A <% QueryType](query: A, fields: Fields, skip: Long, limit: Long, batchSize: Int): Future[AsyncCursor[EntityType]] =
        find(query: QueryType, FindOptions(fields = fields.toOption, skip = Some(skip), limit = Some(limit), batchSize = Some(batchSize)))

    final def find(query: QueryType, options: FindOptions): Future[AsyncCursor[EntityType]] =
        underlying.find(query, options)

    final def findOne(): Future[Option[EntityType]] =
        underlying.findOne(EmptyDocument, FindOneOptions.empty)
    final def findOne[A <% QueryType](query: A): Future[Option[EntityType]] =
        findOne(query: QueryType, FindOneOptions.empty)
    final def findOne[A <% QueryType](query: A, fields: Fields): Future[Option[EntityType]] =
        findOne(query: QueryType, FindOneOptions(fields = fields.toOption))

    final def findOne(query: QueryType, options: FindOneOptions): Future[Option[EntityType]] =
        underlying.findOne(query, options)

    final def findOneById(id: IdType): Future[Option[EntityType]] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById(id: IdType, fields: Fields): Future[Option[EntityType]] =
        findOneById(id, FindOneByIdOptions(fields = fields.toOption))

    final def findOneById(id: IdType, options: FindOneByIdOptions): Future[Option[EntityType]] =
        underlying.findOneById(id, options)

    /**
     * Queries mongod for the indexes on this collection.
     */
    final def findIndexes(): Future[AsyncCursor[CollectionIndex]] =
        underlying.findIndexes()
}

trait AsyncCollection[QueryType, EntityType, IdType, ValueType]
    extends ReadOnlyAsyncCollection[QueryType, EntityType, IdType, ValueType]
    with Collection[QueryType, EntityType, IdType, ValueType] {

    protected[mongo] override def underlying: AsyncDriverCollection

    import codecs._

    final def findAndModify(query: QueryType, update: Option[QueryType], options: FindAndModifyOptions[QueryType]): Future[Option[EntityType]] =
        underlying.findAndModify(query, update, options)

    final def findAndReplace[A <% QueryType](query: A, o: EntityType): Future[Option[EntityType]] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions.empty)
    final def findAndReplace[A <% QueryType](query: A, o: EntityType, flags: Set[FindAndModifyFlag]): Future[Option[EntityType]] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](flags = flags))
    final def findAndReplace[A <% QueryType, B <% QueryType](query: A, o: EntityType, sort: B): Future[Option[EntityType]] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](sort = Some(sort)))

    final def findAndReplace[A <% QueryType, B <% QueryType](query: A, o: EntityType, sort: B, flags: Set[FindAndModifyFlag]): Future[Option[EntityType]] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](sort = Some(sort), flags = flags))

    final def findAndReplace[A <% QueryType, B <% QueryType](query: A, o: EntityType, sort: B, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Future[Option[EntityType]] =
        underlying.findAndModify(query: QueryType, Some(o), FindAndModifyOptions[QueryType](sort = Some(sort), fields = fields.toOption, flags = flags))

    final def findAndModify[A <% QueryType, B <% QueryType](query: A, modifier: B): Future[Option[EntityType]] =
        underlying.findAndModify(query: QueryType, Some(modifier: QueryType), FindAndModifyOptions.empty)

    final def findAndModify[A <% QueryType, B <% QueryType](query: A, modifier: B, flags: Set[FindAndModifyFlag]): Future[Option[EntityType]] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](flags = flags))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query: A, modifier: B, sort: C): Future[Option[EntityType]] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort)))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query: A, modifier: B, sort: C, flags: Set[FindAndModifyFlag]): Future[Option[EntityType]] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort), flags = flags))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query: A, modifier: B, sort: C, fields: Fields, flags: Set[FindAndModifyFlag] = Set.empty): Future[Option[EntityType]] =
        findAndModify(query: QueryType, Some(modifier: QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort), fields = fields.toOption, flags = flags))

    final def findAndRemove[A <% QueryType](query: QueryType): Future[Option[EntityType]] =
        findAndModify(query: QueryType, None, FindAndModifyOptions.remove)

    final def findAndRemove[A <% QueryType, B <% QueryType](query: A, sort: B): Future[Option[EntityType]] =
        findAndModify(query: QueryType, None, FindAndModifyOptions[QueryType](sort = Some(sort), flags = Set(FindAndModifyRemove)))

    final def insert(o: EntityType): Future[WriteResult] =
        underlying.insert(o)

    final def save(o: EntityType): Future[WriteResult] =
        underlying.save(o, UpdateOptions.upsert)
    final def update[A <% QueryType](query: A, o: EntityType): Future[WriteResult] =
        underlying.update(query: QueryType, o, UpdateOptions.empty)
    final def updateUpsert[A <% QueryType](query: A, o: EntityType): Future[WriteResult] =
        underlying.updateUpsert(query: QueryType, o, UpdateOptions.upsert)

    final def update[A <% QueryType, B <% QueryType](query: A, modifier: B): Future[WriteResult] =
        update(query: QueryType, modifier, UpdateOptions.empty)

    final def update(query: QueryType, modifier: QueryType, options: UpdateOptions): Future[WriteResult] =
        underlying.update(query, modifier, options)

    final def updateUpsert[A <% QueryType, B <% QueryType](query: A, modifier: B): Future[WriteResult] =
        update(query: QueryType, modifier, UpdateOptions.upsert)
    final def updateMulti[A <% QueryType, B <% QueryType](query: A, modifier: B): Future[WriteResult] =
        update(query: QueryType, modifier, UpdateOptions.multi)

    final def remove(query: QueryType): Future[WriteResult] =
        underlying.remove(query)

    final def removeById(id: IdType): Future[WriteResult] =
        underlying.removeById(id)

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex(keys: QueryType): Future[WriteResult] =
        ensureIndex(keys, IndexOptions.empty)
    /**
     * Creates the given index on the collection, using custom options.
     */
    final def ensureIndex(keys: QueryType, options: IndexOptions): Future[WriteResult] =
        underlying.ensureIndex(keys, options)

    final def dropIndexes(): Future[CommandResult] = dropIndex("*")

    /**
     * Removes the given index from the collection.
     */
    final def dropIndex(name: String): Future[CommandResult] =
        underlying.dropIndex(name)
}

object AsyncCollection {

    private class AsyncCollectionImpl[QueryType, EntityType, IdType, ValueType](name: String, override val context: Context,
        override val codecs: CollectionCodecSet[QueryType, EntityType, IdType, ValueType])
        extends AsyncCollection[QueryType, EntityType, IdType, ValueType] {
        override val underlying = context.driver.newAsyncCollection(name)(context.driverContext)
    }

    def apply[QueryType, EntityType, IdType, ValueType](name: String, codecs: CollectionCodecSet[QueryType, EntityType, IdType, ValueType])(implicit context: Context): AsyncCollection[QueryType, EntityType, IdType, ValueType] = {
        new AsyncCollectionImpl(name, context, codecs)
    }
}
