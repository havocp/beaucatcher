package org.beaucatcher.driver

private[beaucatcher] trait DriverContext {
    type DriverType <: Driver
    type DatabaseType <: DriverDatabase
    type UnderlyingConnectionType
    type UnderlyingDatabaseType
    type UnderlyingCollectionType

    def underlyingConnection: UnderlyingConnectionType
    def underlyingDatabase: UnderlyingDatabaseType
    def underlyingCollection(name: String): UnderlyingCollectionType

    def driver: DriverType

    def database: DatabaseType

    /**
     * Should close the connection to Mongo; will break any code currently trying to use this context,
     * or any collection or database objects that point to this context.
     */
    def close(): Unit
}
