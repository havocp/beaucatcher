package org.beaucatcher.mongo
import org.beaucatcher.bson.BObject

trait MongoBackendProvider {
    def backend : MongoBackend
}

trait MongoBackend {
    type ConnectionType
    type DatabaseType
    type CollectionType

    def underlyingConnection : ConnectionType
    def underlyingDatabase : DatabaseType
    def underlyingCollection(name : String) : CollectionType

    def createDAOGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        entityBObjectQueryComposer : QueryComposer[BObject, BObject],
        entityBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType]
}
