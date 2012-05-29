package org.beaucatcher.mongo.jdriver

import org.beaucatcher.mongo.SyncCollection
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.actor.ActorSystem
import com.typesafe.config._

trait JavaDriverTestContextProvider
    extends ContextProvider {
    def mongoContext = JavaTestContexts.testContext
}

trait JavaDriverTestDatabaseContextProvider
    extends ContextProvider {
    def mongoContext = JavaTestContexts.testDatabaseContext
}

object JavaTestContexts {
    import scala.collection.JavaConverters._

    private def config(dbname: String): Config = {
        ConfigFactory.parseMap(Map(
            "beaucatcher.mongo.driver" -> "org.beaucatcher.mongo.jdriver.JavaDriver",
            "beaucatcher.mongo.uri" -> ("mongodb://localhost:27017/" + dbname)).asJava)
            .withFallback(ConfigFactory.defaultReference())
    }

    lazy val testContext = Context(config("beaucatcherjdriver"))
    lazy val testDatabaseContext = Context(config("beaucatcherjdriverdb"))
}
