package org.beaucatcher.mongo

import akka.dispatch._
import akka.pattern._
import akka.util._
import akka.util.duration._

final private class SyncCollectionWrappingAsync[QueryType, EntityType, IdType, ValueType](val underlying : AsyncCollection[QueryType, EntityType, IdType, ValueType])
    extends SyncCollection[QueryType, EntityType, IdType, ValueType] {

    // FIXME make this configurable
    final private val timeout = 31 seconds

    final private def block[T](future : Future[T]) : T = {
        Await.result(future, timeout)
    }

    private[beaucatcher] final override def underlyingAsync =
        Some(underlying)

    final override def context =
        underlying.context

    final override def name =
        underlying.name

    final override def emptyQuery =
        underlying.emptyQuery

    final override def entityToUpsertableObject(entity : EntityType) =
        underlying.entityToUpsertableObject(entity)

    final override def entityToModifierObject(entity : EntityType) =
        underlying.entityToModifierObject(entity)

    final override def entityToUpdateQuery(entity : EntityType) =
        underlying.entityToUpdateQuery(entity)

    final override def count(query : QueryType, options : CountOptions) : Long =
        block(underlying.count(query, options))

    final override def distinct(key : String, options : DistinctOptions[QueryType]) : Iterator[ValueType] =
        block(underlying.distinct(key, options)).map(block(_))

    final override def find(query : QueryType, options : FindOptions) : Iterator[EntityType] =
        block(underlying.find(query, options)).map(block(_))

    final override def findOne(query : QueryType, options : FindOneOptions) : Option[EntityType] =
        block(underlying.findOne(query, options))

    final override def findOneById(id : IdType, options : FindOneByIdOptions) : Option[EntityType] =
        block(underlying.findOneById(id, options))

    final override def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Option[EntityType] =
        block(underlying.findAndModify(query, update, options))

    final override def insert(o : EntityType) : WriteResult =
        block(underlying.insert(o))

    final override def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : WriteResult =
        block(underlying.update(query, modifier, options))

    final override def remove(query : QueryType) : WriteResult =
        block(underlying.remove(query))

    final override def removeById(id : IdType) : WriteResult =
        block(underlying.removeById(id))

    final override def ensureIndex(keys : QueryType, options : IndexOptions) : WriteResult =
        block(underlying.ensureIndex(keys, options))

    final override def dropIndex(indexName : String) : CommandResult =
        block(underlying.dropIndex(indexName))
}
