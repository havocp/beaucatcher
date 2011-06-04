package com.ometer.casbah

import com.ometer.mongo._
import com.ometer.bson._
import com.ometer.bson.Implicits._
import com.mongodb.casbah.MongoCollection
import org.bson.types.ObjectId

abstract class CasbahCollectionOperationsWithTransformedId[EntityType <: Product : Manifest, CaseClassIdType, BObjectIdType]
    extends CollectionOperations[EntityType, CaseClassIdType, BObjectIdType] {

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

    /**
     * You would have to override this if your ID type changes between the case class
     * and BObject layers.
     */
    protected val caseClassBObjectIdComposer : IdComposer[CaseClassIdType, BObjectIdType]

    /* If this isn't lazy, then caseClassBObjectIdComposer is null, I guess because
     * the superclass is initialized prior to the subclass.
     */
    private lazy val daoGroup =
        new CaseClassBObjectCasbahDAOGroup[EntityType, CaseClassIdType, BObjectIdType](collection,
            caseClassBObjectQueryComposer,
            caseClassBObjectEntityComposer,
            caseClassBObjectIdComposer)

    final override protected val manifestOfEntityType = manifest[EntityType]

    final override lazy val bobjectSyncDAO = daoGroup.bobjectSyncDAO
    final override lazy val caseClassSyncDAO = daoGroup.caseClassSyncDAO
}

/**
 * Any IdType that can be treated as a BValue should work, though in the current implementation
 * (which just passes the ID value directly to Casbah without actually creating a BValue)
 * some cases might not actually work.
 */
abstract class CasbahCollectionOperations[EntityType <: Product : Manifest, IdType <% BValue]
    extends CasbahCollectionOperationsWithTransformedId[EntityType, IdType, IdType] {
    override protected val caseClassBObjectIdComposer = new IdentityIdComposer[IdType]
}

/**
 * This is the most common case, where the ID is just an ObjectId.
 */
abstract class CasbahCollectionOperationsWithObjectId[EntityType <: Product : Manifest]
    extends CasbahCollectionOperations[EntityType, ObjectId] {

}
