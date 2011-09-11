package org.beaucatcher.mongo
import org.beaucatcher.bson.BObject

trait MongoBackendProvider {
    def backend : MongoBackend
}

trait MongoBackend {

    def createDAOGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        entityBObjectQueryComposer : QueryComposer[BObject, BObject],
        entityBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType]
}
