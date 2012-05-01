package org.beaucatcher.driver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.actor.ActorSystem

/**
 * A mixin trait that provides a [[org.beaucatcher.driver.Driver]] to the class
 * you mix it into.
 */
trait DriverProvider {
    def mongoDriver: Driver
}

/**
 * An opaque object representing the implementation backend for Mongo requests.
 */
trait Driver {

    // TODO These codec set methods are a temporary hack; we don't really want the drivers
    // to know about BObject or case classes.
    private[beaucatcher] def newBObjectCodecSet[IdType: IdEncoder](): CollectionCodecSet[BObject, BObject, IdType, BValue]

    private[beaucatcher] def newCaseClassCodecSet[EntityType <: Product: Manifest, IdType: IdEncoder](): CollectionCodecSet[BObject, EntityType, IdType, Any]

    private[beaucatcher] def newStringIdEncoder(): IdEncoder[String]

    private[beaucatcher] def newObjectIdIdEncoder(): IdEncoder[ObjectId]

    private[beaucatcher] def newBObjectBasedCodecs[E](toBObject: (E) => BObject,
        fromBObject: (BObject) => E): BObjectBasedCodecs[E]

    private[beaucatcher] def newSyncCollection(name: String)(implicit context: DriverContext): SyncDriverCollection

    private[beaucatcher] def newAsyncCollection(name: String)(implicit context: DriverContext): AsyncDriverCollection

    private[beaucatcher] def newContext(url: String, system: ActorSystem): DriverContext
}
