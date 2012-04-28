package org.beaucatcher.mongo.cdriver

import org.beaucatcher.mongo.SyncCollection
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.cdriver._
import akka.actor.ActorSystem

trait ChannelDriverTestContextProvider
    extends ContextProvider {
    def mongoContext = ChannelTestContexts.testContext
}

trait ChannelDriverTestDatabaseContextProvider
    extends ContextProvider {
    def mongoContext = ChannelTestContexts.testDatabaseContext
}

object ChannelTestContexts
    extends ChannelDriverProvider {
    private val actorSystem = ActorSystem("beaucatchercdrivertest")

    lazy val testContext = mongoDriver.newContext(new SimpleMongoConfig("beaucatchercdriver", "localhost", 27017),
        actorSystem)
    lazy val testDatabaseContext = mongoDriver.newContext(new SimpleMongoConfig("beaucatchercdriverdb", "localhost", 27017),
        actorSystem)
}
