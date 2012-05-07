package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.actor.ActorSystem
import com.typesafe.config._

trait ChannelDriverTestContextProvider
    extends ContextProvider {
    def mongoContext = ChannelTestContexts.testContext
}

trait ChannelDriverTestDatabaseContextProvider
    extends ContextProvider {
    def mongoContext = ChannelTestContexts.testDatabaseContext
}

object ChannelTestContexts {
    import scala.collection.JavaConverters._

    private def config(dbname: String): Config = {
        ConfigFactory.parseMap(Map("beaucatcher.mongo.uri" -> ("mongodb://localhost:27017/" + dbname)).asJava)
            .withFallback(ConfigFactory.defaultReference())
    }

    lazy val testContext = Context(config("beaucatchercdriver"))
    lazy val testDatabaseContext = Context(config("beaucatchercdriverdb"))
}
