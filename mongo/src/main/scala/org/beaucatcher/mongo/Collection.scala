package org.beaucatcher.mongo

import org.beaucatcher.driver._

/**
 * Common base class between ReadOnlySyncCollection and ReadOnlyAsyncCollection containing
 * operations that are always synchronous.
 */
trait ReadOnlyCollection {

    protected[mongo] def context: Context

    protected[mongo] implicit final def implicitContext = context

    protected[mongo] def underlying: ReadOnlyDriverCollection

    /** The database containing the collection */
    final def database: Database = context.database

    /** The name of the collection */
    def name: String = underlying.name

    /** The name of the collection with database included, like "databaseName.collectionName" */
    def fullName: String = underlying.fullName
}

/**
 * Common base class between BoundReadOnlySyncCollection and BoundReadOnlyAsyncCollection, holding
 * only those operations that are always synchronous. Bound collections have codecs pre-associated.
 */
trait BoundReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {

    protected[mongo] def unbound: ReadOnlyCollection

    protected[mongo] implicit def codecs: ReadOnlyCollectionCodecSet[QueryType, EntityType, IdType, ValueType]

    /** The database containing the collection */
    final def database: Database = unbound.database

    /** The name of the collection */
    final def name: String = unbound.name

    /** The name of the collection with database included, like "databaseName.collectionName" */
    final def fullName: String = unbound.fullName
}

/**
 * Common base class between SyncCollection and AsyncCollection,
 * holding only those operations that are always synchronous.
 */
trait Collection extends ReadOnlyCollection {
    protected[mongo] override def underlying: DriverCollection
}

/**
 * Common base class between BoundSyncCollection and BoundAsyncCollection,
 * holding only those operations that are always synchronous.
 */
trait BoundCollection[QueryType, EntityType, IdType, ValueType]
    extends BoundReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {
    protected[mongo] override def unbound: Collection

    protected[mongo] override implicit def codecs: CollectionCodecSet[QueryType, EntityType, EntityType, IdType, ValueType]
}
