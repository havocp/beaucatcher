package org.beaucatcher.mongo
import org.beaucatcher.bson.BObject

trait MongoBackendProvider {
    def backend : MongoBackend
}

trait MongoBackend {

    def createDAOGroup[EntityType <: Product : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType]
}
