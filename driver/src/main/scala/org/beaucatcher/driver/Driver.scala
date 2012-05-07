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

    private[beaucatcher] def newSyncCollection(name: String)(implicit context: DriverContext): SyncDriverCollection

    private[beaucatcher] def newAsyncCollection(name: String)(implicit context: DriverContext): AsyncDriverCollection

    private[beaucatcher] def newContext(url: String, system: ActorSystem): DriverContext
}
