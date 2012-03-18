package org.beaucatcher.mongo

import org.beaucatcher.bson._
import akka.actor.ActorSystem

/**
 * A mixin trait that provides a [[org.beaucatcher.mongo.Driver]] to the class
 * you mix it into.
 */
trait DriverProvider {
    def mongoDriver : Driver
}

trait Driver {

    def createCollectionGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        entityBObjectQueryComposer : QueryComposer[BObject, BObject],
        entityBObjectEntityComposer : EntityComposer[EntityType, BObject]) : CollectionGroup[EntityType, IdType, IdType]

    def createCollectionGroupWithoutEntity[IdType : Manifest](collectionName : String) : CollectionGroupWithoutEntity[IdType]

    def newContext(config : MongoConfig, system : ActorSystem) : Context
}
