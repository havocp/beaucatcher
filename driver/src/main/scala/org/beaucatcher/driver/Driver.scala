package org.beaucatcher.driver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.actor.ActorSystem

/**
 * An opaque object representing the implementation backend for Mongo requests.
 */
trait Driver {

    private[beaucatcher] def newSyncCollection(name: String)(implicit context: DriverContext): SyncDriverCollection

    private[beaucatcher] def newAsyncCollection(name: String)(implicit context: DriverContext): AsyncDriverCollection

    private[beaucatcher] def newContext(url: String, system: ActorSystem): DriverContext
}
