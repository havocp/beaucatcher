package org.beaucatcher.mongo
import org.beaucatcher.bson.BObject

/**
 * A mixin trait that provides a [[org.beaucatcher.mongo.MongoBackend]] to the class
 * you mix it into.
 */
trait MongoBackendProvider {
    def backend : MongoBackend
}

/**
 * A [[org.beaucatcher.mongo.MongoBackend]] represents an underlying Mongo protocol implementation, a connection
 * pool, and a single named database on that connection. The backend lets you work with the database and with
 * collections inside that database. To use a backend you need to use a concrete subclass such as
 * [[org.beaucatcher.casbah.CasbahMongoBackend]], often via a [[org.beaucatcher.mongo.MongoBackendProvider]] such as
 * [[org.beaucatcher.casbah.CasbahBackendProvider]].
 */
trait MongoBackend {
    type ConnectionType
    type DatabaseType
    type CollectionType

    def underlyingConnection : ConnectionType
    def underlyingDatabase : DatabaseType
    def underlyingCollection(name : String) : CollectionType

    def createCollectionGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        entityBObjectQueryComposer : QueryComposer[BObject, BObject],
        entityBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncCollectionGroup[EntityType, IdType, IdType]

    def createCollectionGroupWithoutEntity[IdType : Manifest](collectionName : String) : SyncCollectionGroupWithoutEntity[IdType]

    def database : Database
    def config : MongoConfig
}
