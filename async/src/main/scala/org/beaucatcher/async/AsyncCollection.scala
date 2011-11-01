package org.beaucatcher.async

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo._
import akka.dispatch.Future

abstract trait AsyncCollection[QueryType, EntityType, IdType, ValueType] {
    private[beaucatcher] def backend : MongoBackend

    /** The database containing the collection */
    final def database : Database = backend.database

    def name : String

    def fullName : String

    def emptyQuery : QueryType

    final def count() : Future[Long] =
        count(emptyQuery)
    final def count[A <% QueryType](query : A) : Future[Long] =
        count(query : QueryType, CountOptions.empty)
    final def count[A <% QueryType](query : A, fields : Fields) : Future[Long] =
        count(query : QueryType, CountOptions(fields = fields.toOption))

    def count(query : QueryType, options : CountOptions) : Future[Long]

    // FIXME shouldn't distinct return an iterator not a seq for scalability (so it can be lazy?)
    final def distinct(key : String) : Future[Seq[ValueType]] =
        distinct(key, DistinctOptions.empty)
    final def distinct[A <% QueryType](key : String, query : A) : Future[Seq[ValueType]] =
        distinct(key, DistinctOptions[QueryType](query = Some(query)))

    def distinct(key : String, options : DistinctOptions[QueryType]) : Future[Seq[ValueType]]

    final def find() : Future[Iterator[Future[EntityType]]] =
        find(emptyQuery, FindOptions.empty)
    final def find[A <% QueryType](query : A) : Future[Iterator[Future[EntityType]]] =
        find(query : QueryType, FindOptions.empty)
    final def find[A <% QueryType](query : A, fields : Fields) : Future[Iterator[Future[EntityType]]] =
        find(query : QueryType, FindOptions(fields = fields.toOption))
    final def find[A <% QueryType](query : A, fields : Fields, skip : Long, limit : Long, batchSize : Int) : Future[Iterator[Future[EntityType]]] =
        find(query : QueryType, FindOptions(fields = fields.toOption, skip = Some(skip), limit = Some(limit), batchSize = Some(batchSize)))

    def find(query : QueryType, options : FindOptions) : Future[Iterator[Future[EntityType]]]

    final def findOne() : Future[Option[EntityType]] =
        findOne(emptyQuery, FindOneOptions.empty)
    final def findOne[A <% QueryType](query : A) : Future[Option[EntityType]] =
        findOne(query : QueryType, FindOneOptions.empty)
    final def findOne[A <% QueryType](query : A, fields : Fields) : Future[Option[EntityType]] =
        findOne(query : QueryType, FindOneOptions(fields = fields.toOption))

    def findOne(query : QueryType, options : FindOneOptions) : Future[Option[EntityType]]

    final def findOneById(id : IdType) : Future[Option[EntityType]] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById(id : IdType, fields : Fields) : Future[Option[EntityType]] =
        findOneById(id, FindOneByIdOptions(fields = fields.toOption))

    def findOneById(id : IdType, options : FindOneByIdOptions) : Future[Option[EntityType]]

    final def findAndReplace[A <% QueryType](query : A, o : EntityType) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions.empty)
    final def findAndReplace[A <% QueryType](query : A, o : EntityType, flags : Set[FindAndModifyFlag]) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](flags = flags))
    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](sort = Some(sort)))

    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B, flags : Set[FindAndModifyFlag]) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](sort = Some(sort), flags = flags))

    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B, fields : Fields, flags : Set[FindAndModifyFlag] = Set.empty) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](sort = Some(sort), fields = fields.toOption, flags = flags))

    final def findAndModify[A <% QueryType, B <% QueryType](query : A, modifier : B) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions.empty)

    final def findAndModify[A <% QueryType, B <% QueryType](query : A, modifier : B, flags : Set[FindAndModifyFlag]) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](flags = flags))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort)))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C, flags : Set[FindAndModifyFlag]) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort), flags = flags))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C, fields : Fields, flags : Set[FindAndModifyFlag] = Set.empty) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](sort = Some(sort), fields = fields.toOption, flags = flags))

    final def findAndRemove[A <% QueryType](query : QueryType) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, None, FindAndModifyOptions.remove)

    final def findAndRemove[A <% QueryType, B <% QueryType](query : A, sort : B) : Future[Option[EntityType]] =
        findAndModify(query : QueryType, None, FindAndModifyOptions[QueryType](sort = Some(sort), flags = Set(FindAndModifyRemove)))

    def entityToUpsertableObject(entity : EntityType) : QueryType
    def entityToModifierObject(entity : EntityType) : QueryType

    def entityToUpdateQuery(entity : EntityType) : QueryType

    def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Future[Option[EntityType]]

    def insert(o : EntityType) : Future[WriteResult]

    final def save(o : EntityType) : Future[WriteResult] =
        updateUpsert(entityToUpdateQuery(o), entityToUpsertableObject(o))
    final def update[A <% QueryType](query : A, o : EntityType) : Future[WriteResult] =
        update(query : QueryType, entityToModifierObject(o), UpdateOptions.empty)
    final def updateUpsert[A <% QueryType](query : A, o : EntityType) : Future[WriteResult] =
        update(query : QueryType, entityToUpsertableObject(o), UpdateOptions.upsert)

    final def update[A <% QueryType, B <% QueryType](query : A, modifier : B) : Future[WriteResult] =
        update(query : QueryType, modifier, UpdateOptions.empty)
    final def updateUpsert[A <% QueryType, B <% QueryType](query : A, modifier : B) : Future[WriteResult] =
        update(query : QueryType, modifier, UpdateOptions.upsert)
    final def updateMulti[A <% QueryType, B <% QueryType](query : A, modifier : B) : Future[WriteResult] =
        update(query : QueryType, modifier, UpdateOptions.multi)
    def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : Future[WriteResult]

    def remove(query : QueryType) : Future[WriteResult]

    def removeById(id : IdType) : Future[WriteResult]

    /**
     * Creates the given index on the collection (if it hasn't already been created),
     * using default options.
     */
    final def ensureIndex(keys : QueryType) : Future[WriteResult] =
        ensureIndex(keys, IndexOptions.empty)

    /**
     * Creates the given index on the collection, using custom options.
     */
    def ensureIndex(keys : QueryType, options : IndexOptions) : Future[WriteResult]

    final def dropIndexes() : Future[CommandResult] = dropIndex("*")

    /**
     * Removes the given index from the collection.
     */
    def dropIndex(name : String) : Future[CommandResult]

    /**
     * Queries mongod for the indexes on this collection.
     */
    def findIndexes() : Future[Iterator[Future[CollectionIndex]]]
}

object AsyncCollection {

}
