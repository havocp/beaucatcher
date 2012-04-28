package org.beaucatcher.mongo

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
    def name : String

    // this has to be lazy since "database" may not be
    // valid yet during construct
    private lazy val _fullName = database.name + "." + name

    /**
     * The name of the collection with database included, like "databaseName.collectionName"
     *
     */
    def fullName : String = _fullName

    /** Construct an empty query object */
    def emptyQuery : QueryType
}

/**
 * Common base class between SyncCollection and AsyncCollection,
 * holding only those operations that are always synchronous.
 */
trait Collection[QueryType, EntityType, IdType, ValueType] extends ReadOnlyCollection[QueryType, EntityType, IdType, ValueType] {

    /** An upsertable object generally must have all fields and must have an ID. */
    def entityToUpsertableObject(entity : EntityType) : QueryType
    /**
     * A modifier object generally must not have an ID, and may be missing fields that
     * won't be modified.
     */
    def entityToModifierObject(entity : EntityType) : QueryType
    /**
     * Returns a query that matches only the given entity,
     * such as a query for the entity's ID.
     */
    def entityToUpdateQuery(entity : EntityType) : QueryType
}
