package org.beaucatcher.mongo

import org.beaucatcher.bson._
import akka.actor.ActorSystem
import scala.annotation.implicitNotFound

/**
 * A [[org.beaucatcher.mongo.Context]] represents an underlying Mongo protocol implementation, a connection
 * pool, and a single named database on that connection. It may also include other configuration settings
 * and the Akka ActorSystem.
 *
 * Implementations of [[org.beaucatcher.mongo.Context]] are provided by a "driver" such
 * as [[org.beaucatcher.jdriver.JavaContext]]
 *
 * This object is normally passed around as an implicit.
 */
@implicitNotFound(msg = "No mongo.Context found")
trait Context {
    type DriverType <: Driver
    type DatabaseType <: Database
    type UnderlyingConnectionType
    type UnderlyingDatabaseType
    type UnderlyingCollectionType

    def underlyingConnection : UnderlyingConnectionType
    def underlyingDatabase : UnderlyingDatabaseType
    def underlyingCollection(name : String) : UnderlyingCollectionType

    def driver : DriverType

    def database : DatabaseType

    def config : MongoConfig

    def actorSystem : ActorSystem

    /**
     * Should close the connection to Mongo; will break any code currently trying to use this context,
     * or any collection or database objects that point to this context.
     */
    def close : Unit
}

trait ContextProvider {
    def mongoContext : Context
}
