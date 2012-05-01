package org.beaucatcher.mongo

import org.beaucatcher.driver._

/**
 * Common base class between ReadOnlySyncCollection and ReadOnlyAsyncCollection, holding
 * only those operations that are always synchronous.
 */
trait ReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {

    private[beaucatcher] def context : Context

    protected[this] implicit final def implicitContext = context

    /** The database containing the collection */
    final def database : Database = context.database

    /** The name of the collection */
    def name : String = underlying.name

    /** The name of the collection with database included, like "databaseName.collectionName" */
    def fullName : String = underlying.fullName

    // this has to be a 'val' so it can be imported
    protected[mongo] val codecs : ReadOnlyCollectionCodecSet[QueryType, EntityType, IdType, ValueType]

    protected[mongo] def underlying : ReadOnlyDriverCollection
}

/**
 * Common base class between SyncCollection and AsyncCollection,
 * holding only those operations that are always synchronous.
 */
trait Collection[QueryType, EntityType, IdType, ValueType] extends ReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {
    // this has to be a 'val' so it can be imported
    protected[mongo] override val codecs : CollectionCodecSet[QueryType, EntityType, IdType, ValueType]

    protected[mongo] override def underlying : DriverCollection
}
