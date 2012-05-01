package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.driver._
import akka.actor.ActorSystem
import scala.annotation.implicitNotFound

/**
 * A [[org.beaucatcher.mongo.Context]] represents an underlying Mongo protocol implementation, a connection
 * pool, and a single named database on that connection. It may also include other configuration settings
 * and the Akka ActorSystem.
 *
 * Implementations of [[org.beaucatcher.mongo.Context]] are provided by a "driver" such
 * as [[org.beaucatcher.jdriver.JavaDriverContext]].
 *
 * This object is normally passed around as an implicit.
 */
@implicitNotFound(msg = "No mongo.Context found")
trait Context {
    type DriverType <: Driver
    type UnderlyingConnectionType
    type UnderlyingDatabaseType
    type UnderlyingCollectionType

    def underlyingConnection : UnderlyingConnectionType
    def underlyingDatabase : UnderlyingDatabaseType
    def underlyingCollection(name : String) : UnderlyingCollectionType

    private[beaucatcher] def driverContext : DriverContext

    def driver : DriverType

    def database : Database

    def config : MongoConfig

    def actorSystem : ActorSystem

    /**
     * Should close the connection to Mongo; will break any code currently trying to use this context,
     * or any collection or database objects that point to this context.
     */
    def close : Unit
}

trait ContextProvider extends DriverProvider {
    def mongoContext : Context
    override final def mongoDriver = mongoContext.driver
}

object Context {
    def apply[D <: Driver](driver : D, config : MongoConfig, system : ActorSystem) : Context = {
        val outerDriver = driver
        val outerConfig = config
        new Context() {
            override val driver = outerDriver
            override lazy val driverContext = driver.newContext(config.url, system)
            override lazy val database = Database(this)
            override val config = outerConfig
            override val actorSystem = system
            override def close() : Unit = driverContext.close()

            override type DriverType = D
            override type UnderlyingConnectionType = driverContext.UnderlyingConnectionType
            override type UnderlyingDatabaseType = driverContext.UnderlyingDatabaseType
            override type UnderlyingCollectionType = driverContext.UnderlyingCollectionType

            override def underlyingConnection = driverContext.underlyingConnection
            override def underlyingDatabase = driverContext.underlyingDatabase
            override def underlyingCollection(name : String) = driverContext.underlyingCollection(name)

        }
    }
}
