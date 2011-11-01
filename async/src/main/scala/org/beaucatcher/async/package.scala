package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.actor.Actor
import akka.dispatch.Future

package object async {

    private class AsyncCollectionWrappingSync[QueryType, EntityType, IdType, ValueType](private[async] val underlying : SyncCollection[QueryType, EntityType, IdType, ValueType])
        extends AsyncCollection[QueryType, EntityType, IdType, ValueType] {
        // FIXME use one global pool, not a different one per Collection; pass the underlying sync Collection
        // in the messages.
        private val actor = Actor.actorOf(new CollectionActor[QueryType, EntityType, IdType, ValueType](underlying)).start

        override def backend =
            underlying.backend
        override def name =
            underlying.name
        override def fullName =
            underlying.fullName
        override def emptyQuery : QueryType =
            underlying.emptyQuery
        override def count(query : QueryType, options : CountOptions) : Future[Long] =
            actor !!! CountRequest(query, options)
        override def distinct(key : String, options : DistinctOptions[QueryType]) : Future[Seq[ValueType]] =
            actor !!! DistinctRequest(key, options)
        override def find(query : QueryType, options : FindOptions) : Future[Iterator[Future[EntityType]]] = {
            val futureIterator : Future[Iterator[EntityType]] = actor !!! FindRequest(query, options)
            futureIterator map { _ map { Future(_) } }
        }
        override def findOne(query : QueryType, options : FindOneOptions) : Future[Option[EntityType]] =
            actor !!! FindOneRequest(query, options)
        override def findOneById(id : IdType, options : FindOneByIdOptions) : Future[Option[EntityType]] =
            actor !!! FindOneByIdRequest(id, options)
        override def entityToUpsertableObject(entity : EntityType) : QueryType =
            underlying.entityToUpsertableObject(entity)
        override def entityToModifierObject(entity : EntityType) : QueryType =
            underlying.entityToModifierObject(entity)
        override def entityToUpdateQuery(entity : EntityType) : QueryType =
            underlying.entityToUpdateQuery(entity)
        override def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Future[Option[EntityType]] =
            actor !!! FindAndModifyRequest(query, update, options)
        override def insert(o : EntityType) : Future[WriteResult] =
            actor !!! InsertRequest(o)
        override def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : Future[WriteResult] =
            actor !!! UpdateRequest(query, modifier, options)
        override def remove(query : QueryType) : Future[WriteResult] =
            actor !!! RemoveRequest(query)
        override def removeById(id : IdType) : Future[WriteResult] =
            actor !!! RemoveByIdRequest(id)
        override def ensureIndex(keys : QueryType, options : IndexOptions) : Future[WriteResult] =
            actor !!! EnsureIndexRequest(keys, options)
        override def dropIndex(name : String) : Future[CommandResult] =
            actor !!! DropIndexRequest(name)
        override def findIndexes() : Future[Iterator[Future[CollectionIndex]]] = {
            val futureIterator : Future[Iterator[CollectionIndex]] = actor !!! FindIndexesRequest
            futureIterator map { _ map { Future(_) } }
        }
    }

    private class SyncCollectionWrappingAsync[QueryType, EntityType, IdType, ValueType](private[async] val underlying : AsyncCollection[QueryType, EntityType, IdType, ValueType])
        extends SyncCollection[QueryType, EntityType, IdType, ValueType] {
        override def backend =
            underlying.backend
        override def name =
            underlying.name
        override def fullName =
            underlying.fullName
        override def emptyQuery : QueryType =
            underlying.emptyQuery
        override def count(query : QueryType, options : CountOptions) : Long =
            underlying.count(query, options).get
        override def distinct(key : String, options : DistinctOptions[QueryType]) : Seq[ValueType] =
            underlying.distinct(key, options).get
        override def find(query : QueryType, options : FindOptions) : Iterator[EntityType] =
            underlying.find(query, options).get map { _.get }
        override def findOne(query : QueryType, options : FindOneOptions) : Option[EntityType] =
            underlying.findOne(query, options).get
        override def findOneById(id : IdType, options : FindOneByIdOptions) : Option[EntityType] =
            underlying.findOneById(id, options).get
        override def entityToUpsertableObject(entity : EntityType) : QueryType =
            underlying.entityToUpsertableObject(entity)
        override def entityToModifierObject(entity : EntityType) : QueryType =
            underlying.entityToModifierObject(entity)
        override def entityToUpdateQuery(entity : EntityType) : QueryType =
            underlying.entityToUpdateQuery(entity)
        override def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Option[EntityType] =
            underlying.findAndModify(query, update, options).get
        override def insert(o : EntityType) : WriteResult =
            underlying.insert(o).get
        override def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : WriteResult =
            underlying.update(query, modifier, options).get
        override def remove(query : QueryType) : WriteResult =
            underlying.remove(query).get
        override def removeById(id : IdType) : WriteResult =
            underlying.removeById(id).get
        override def ensureIndex(keys : QueryType, options : IndexOptions) : WriteResult =
            underlying.ensureIndex(keys, options).get
        override def dropIndex(name : String) : CommandResult =
            underlying.dropIndex(name).get
    }

    def makeAsync[QueryType, EntityType, IdType, ValueType](sync : SyncCollection[QueryType, EntityType, IdType, ValueType]) : AsyncCollection[QueryType, EntityType, IdType, ValueType] = {
        sync match {
            case wrapper : SyncCollectionWrappingAsync[_, _, _, _] =>
                // we can just "unwrap" to avoid building up a huge chain
                wrapper.asInstanceOf[SyncCollectionWrappingAsync[QueryType, EntityType, IdType, ValueType]].underlying
            case _ =>
                new AsyncCollectionWrappingSync(sync)
        }
    }

    def makeSync[QueryType, EntityType, IdType, ValueType](async : AsyncCollection[QueryType, EntityType, IdType, ValueType]) : SyncCollection[QueryType, EntityType, IdType, ValueType] = {
        async match {
            case wrapper : AsyncCollectionWrappingSync[_, _, _, _] =>
                // we can just "unwrap" to avoid building up a huge chain
                wrapper.asInstanceOf[AsyncCollectionWrappingSync[QueryType, EntityType, IdType, ValueType]].underlying
            case _ =>
                new SyncCollectionWrappingAsync(async)
        }
    }
}
