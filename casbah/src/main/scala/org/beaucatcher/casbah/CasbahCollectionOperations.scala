package org.beaucatcher.casbah

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import com.mongodb.casbah.MongoCollection
import org.bson.types.ObjectId

abstract class CasbahCollectionOperations[EntityType <: Product : Manifest, IdType]
    extends CollectionOperations[EntityType, IdType] {

    override final val entityTypeManifest = manifest[EntityType]

    /** Implement this field in subclass to attach to a Casbah collection */
    protected val collection : MongoCollection

    /* If this isn't lazy, then caseClassBObjectIdComposer is null, I guess because
     * the superclass is initialized prior to the subclass.
     */
    protected override lazy val daoGroup =
        new CaseClassBObjectCasbahDAOGroup[EntityType, IdType, IdType](collection,
            caseClassBObjectQueryComposer,
            caseClassBObjectEntityComposer,
            new IdentityIdComposer[IdType])
}

/**
 * This is the most common case, where the ID is just an ObjectId.
 */
abstract class CasbahCollectionOperationsWithObjectId[EntityType <: Product : Manifest]
    extends CasbahCollectionOperations[EntityType, ObjectId] {

}
