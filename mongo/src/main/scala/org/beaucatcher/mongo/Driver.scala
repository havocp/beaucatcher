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

    def createCollectionFactory[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        entityBObjectQueryComposer : QueryComposer[BObject, BObject],
        entityBObjectEntityComposer : EntityComposer[EntityType, BObject]) : CollectionFactory[EntityType, IdType, IdType]

    def createCollectionFactoryWithoutEntity[IdType : Manifest](collectionName : String) : CollectionFactoryWithoutEntity[IdType]

    def newContext(config : MongoConfig, system : ActorSystem) : Context
}
