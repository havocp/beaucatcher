package org.beaucatcher.casbah

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import com.mongodb.casbah.MongoCollection
import org.bson.types.ObjectId

abstract class CasbahCollectionOperations[EntityType <: Product : Manifest, IdType]
    extends CollectionOperations[EntityType, IdType] {

    /** Implement this field in subclass to attach to a Casbah collection */
    protected val collection : MongoCollection

    /**
     * There probably isn't a reason to override this, but it would modify a query
     * as it went from the case class DAO to the BObject DAO.
     */
    protected val caseClassBObjectQueryComposer : QueryComposer[BObject, BObject] =
        new IdentityQueryComposer()

    /**
     * You would override this if you want to adjust how a BObject is mapped to a
     * case class entity. For example if you need to deal with missing fields or
     * database format changes, you could do that in this composer. Or if you
     * wanted to do a type mapping, say from Int to an enumeration, you could do that
     * here. Many things you might do with an annotation in something like JPA
     * could instead be done by subclassing CaseClassBObjectEntityComposer, in theory.
     */
    protected val caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject] =
        new CaseClassBObjectEntityComposer[EntityType]

    /* If this isn't lazy, then caseClassBObjectIdComposer is null, I guess because
     * the superclass is initialized prior to the subclass.
     */
    private lazy val daoGroup =
        new CaseClassBObjectCasbahDAOGroup[EntityType, IdType, IdType](collection,
            caseClassBObjectQueryComposer,
            caseClassBObjectEntityComposer,
            new IdentityIdComposer[IdType])

    final override protected val manifestOfEntityType = manifest[EntityType]

    final override lazy val bobjectSyncDAO = daoGroup.bobjectSyncDAO
    final override lazy val caseClassSyncDAO = daoGroup.caseClassSyncDAO
}

/**
 * This is the most common case, where the ID is just an ObjectId.
 */
abstract class CasbahCollectionOperationsWithObjectId[EntityType <: Product : Manifest]
    extends CasbahCollectionOperations[EntityType, ObjectId] {

}
