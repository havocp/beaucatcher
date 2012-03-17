package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import akka.dispatch._
import akka.actor._

private trait AsyncCollectionTypedActor[QueryType, EntityType, IdType, ValueType] {

    def count(query : QueryType, options : CountOptions) : Future[Long]

    def distinct(key : String, options : DistinctOptions[QueryType]) : Future[Seq[ValueType]]

    def find(query : QueryType, options : FindOptions) : Future[Iterator[Future[EntityType]]]

    def findOne(query : QueryType, options : FindOneOptions) : Future[Option[EntityType]]

    def findOneById(id : IdType, options : FindOneByIdOptions) : Future[Option[EntityType]]

    def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Future[Option[EntityType]]

    def insert(o : EntityType) : Future[WriteResult]

    def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : Future[WriteResult]

    def remove(query : QueryType) : Future[WriteResult]

    def removeById(id : IdType) : Future[WriteResult]

    def ensureIndex(keys : QueryType, options : IndexOptions) : Future[WriteResult]

    def dropIndex(name : String) : Future[CommandResult]

    def findIndexes() : Future[Iterator[Future[CollectionIndex]]]
}

final private class AsyncCollectionImpl[QueryType, EntityType, IdType, ValueType](val underlying : SyncCollection[QueryType, EntityType, IdType, ValueType]) extends AsyncCollectionTypedActor[QueryType, EntityType, IdType, ValueType] {
    import TypedActor.dispatcher

    final override def count(query : QueryType, options : CountOptions) : Future[Long] = {
        Future({ Future.blocking(); underlying.count(query, options) })
    }

    final override def distinct(key : String, options : DistinctOptions[QueryType]) : Future[Seq[ValueType]] = {
        Future({ Future.blocking(); underlying.distinct(key, options) })
    }

    final override def find(query : QueryType, options : FindOptions) : Future[Iterator[Future[EntityType]]] = {
        // FIXME this can block when getting batches
        Future({ Future.blocking(); underlying.find(query, options) }) map { i =>
            i.map(Promise.successful(_))
        }
    }

    final override def findOne(query : QueryType, options : FindOneOptions) : Future[Option[EntityType]] = {
        Future({ Future.blocking(); underlying.findOne(query, options) })
    }

    final override def findOneById(id : IdType, options : FindOneByIdOptions) : Future[Option[EntityType]] = {
        Future({ Future.blocking(); underlying.findOneById(id, options) })
    }

    final override def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Future[Option[EntityType]] = {
        Future({ Future.blocking(); underlying.findAndModify(query, update, options) })
    }

    final override def insert(o : EntityType) : Future[WriteResult] = {
        Future({ Future.blocking(); underlying.insert(o) })
    }

    final override def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : Future[WriteResult] = {
        Future({ Future.blocking(); underlying.update(query, modifier, options) })
    }

    final override def remove(query : QueryType) : Future[WriteResult] = {
        Future({ Future.blocking(); underlying.remove(query) })
    }

    final override def removeById(id : IdType) : Future[WriteResult] = {
        Future({ Future.blocking(); underlying.removeById(id) })
    }

    final override def ensureIndex(keys : QueryType, options : IndexOptions) : Future[WriteResult] = {
        Future({ Future.blocking(); underlying.ensureIndex(keys, options) })
    }

    final override def dropIndex(name : String) : Future[CommandResult] = {
        Future({ Future.blocking(); underlying.dropIndex(name) })
    }

    final override def findIndexes() : Future[Iterator[Future[CollectionIndex]]] = {
        // FIXME this can block when getting batches
        Future({ Future.blocking(); underlying.findIndexes() }) map { i =>
            i.map(Promise.successful(_))
        }
    }
}

final private class AsyncCollectionWrappingSync[QueryType, EntityType, IdType, ValueType](val underlying : SyncCollection[QueryType, EntityType, IdType, ValueType],
    val system : ActorSystem)
    extends AsyncCollection[QueryType, EntityType, IdType, ValueType] {

    private[beaucatcher] final override def underlyingSync =
        Some(underlying)

    final override def backend =
        underlying.backend
    final override def name =
        underlying.name
    final override def fullName =
        underlying.fullName
    final override def emptyQuery : QueryType =
        underlying.emptyQuery
    final override def entityToUpsertableObject(entity : EntityType) : QueryType =
        underlying.entityToUpsertableObject(entity)
    final override def entityToModifierObject(entity : EntityType) : QueryType =
        underlying.entityToModifierObject(entity)
    final override def entityToUpdateQuery(entity : EntityType) : QueryType =
        underlying.entityToUpdateQuery(entity)

    // FIXME
    // 1. it's unclear why we need an actor, when the implementation uses Future({}) to dispatch
    //    every method anyway
    // 2. we have no way to stop this actor (need a close() on collections I guess)
    final val actor : AsyncCollectionTypedActor[QueryType, EntityType, IdType, ValueType] = {
        val props = TypedProps(classOf[AsyncCollectionTypedActor[QueryType, EntityType, IdType, ValueType]],
            new AsyncCollectionImpl[QueryType, EntityType, IdType, ValueType](underlying))
        TypedActor(system).typedActorOf(props)
    }

    final override def count(query : QueryType, options : CountOptions) : Future[Long] =
        actor.count(query, options)

    final override def distinct(key : String, options : DistinctOptions[QueryType]) : Future[Seq[ValueType]] =
        actor.distinct(key, options)

    final override def find(query : QueryType, options : FindOptions) : Future[Iterator[Future[EntityType]]] =
        actor.find(query, options)

    final override def findOne(query : QueryType, options : FindOneOptions) : Future[Option[EntityType]] =
        actor.findOne(query, options)

    final override def findOneById(id : IdType, options : FindOneByIdOptions) : Future[Option[EntityType]] =
        actor.findOneById(id, options)

    final override def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Future[Option[EntityType]] =
        actor.findAndModify(query, update, options)

    final override def insert(o : EntityType) : Future[WriteResult] =
        actor.insert(o)

    final override def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : Future[WriteResult] =
        actor.update(query, modifier, options)

    final override def remove(query : QueryType) : Future[WriteResult] =
        actor.remove(query)

    final override def removeById(id : IdType) : Future[WriteResult] =
        actor.removeById(id)

    final override def ensureIndex(keys : QueryType, options : IndexOptions) : Future[WriteResult] =
        actor.ensureIndex(keys, options)

    final override def dropIndex(name : String) : Future[CommandResult] =
        actor.dropIndex(name)

    final override def findIndexes() : Future[Iterator[Future[CollectionIndex]]] =
        actor.findIndexes()
}
