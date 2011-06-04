package com.ometer.casbah

import com.ometer.mongo._
import com.ometer.ClassAnalysis
import com.ometer.bson.BsonAST.BObject
import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection
import org.bson.types.ObjectId

/**
 * A DAOGroup exposes the entire chain of DAO conversions; you can "tap in" and use the
 * nicest DAO at whatever level is convenient for whatever you're doing. You can also
 * override any of the conversions as the data makes its way up from MongoDB.
 *
 * The long-term idea is to get rid of the Casbah part, and the DAOGroup will
 * have a 2x2 of DAO flavors: sync vs. async, and BObject vs. case entity.
 *
 * Rather than a bunch of annotations specifying how to go from MongoDB to
 * the case entity, there's a theory here that you can override the
 * "composer" objects and do things like validation or dealing with legacy
 * object formats in there.
 */
private[casbah] class CaseClassBObjectCasbahDAOGroup[EntityType <: Product : Manifest, CaseClassIdType, BObjectIdType](
    val collection : MongoCollection,
    val caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
    val caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject],
    val caseClassBObjectIdComposer : IdComposer[CaseClassIdType, BObjectIdType]) {
    require(collection != null)
    require(caseClassBObjectQueryComposer != null)
    require(caseClassBObjectEntityComposer != null)
    require(caseClassBObjectIdComposer != null)

    /* this is not a type parameter because we don't want people to transform ID
     * type between BObject and Casbah; transformations should be done on
     * the case-class-to-bobject layer because we want to keep that layer.
     */
    final private type CasbahIdType = BObjectIdType

    /* Let's not allow changing the BObject-to-Casbah mapping since we want to
     * get rid of Casbah's DBObject. That's why these are private.
     */
    private lazy val bobjectCasbahQueryComposer : QueryComposer[BObject, DBObject] =
        new BObjectCasbahQueryComposer()
    private lazy val bobjectCasbahEntityComposer : EntityComposer[BObject, DBObject] =
        new BObjectCasbahEntityComposer()
    private lazy val bobjectCasbahIdComposer : IdComposer[BObjectIdType, CasbahIdType] =
        new IdentityIdComposer()

    /**
     *  This is the "raw" Casbah DAO, if you need to work with a DBObject for some reason.
     *  This is best avoided because the hope is that Hammersmith would allow us to
     *  eliminate this layer. In fact, we'll make this private...
     */
    private lazy val casbahSyncDAO : CasbahSyncDAO[CasbahIdType] = {
        val outerCollection = collection
        new CasbahSyncDAO[CasbahIdType] {
            override val collection = outerCollection
        }
    }

    /**
     *  This DAO works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. You can also use
     *  the unwrappedAsJava field on BObject to get a Java map, which may work
     *  well with your HTML template system. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    lazy val bobjectSyncDAO : BObjectSyncDAO[BObjectIdType] = {
        new BObjectCasbahSyncDAO[BObjectIdType, CasbahIdType] {
            override val backend = casbahSyncDAO
            override val queryComposer = bobjectCasbahQueryComposer
            override val entityComposer = bobjectCasbahEntityComposer
            override val idComposer = bobjectCasbahIdComposer
        }
    }

    /**
     *  This DAO works with a specified case class, for typesafe access to fields
     *  from within Scala code. You also know that all the fields are present
     *  if the case class was successfully constructed.
     */
    lazy val caseClassSyncDAO : CaseClassSyncDAO[BObject, EntityType, CaseClassIdType] = {
        new CaseClassBObjectSyncDAO[EntityType, CaseClassIdType, BObjectIdType] {
            override val backend = bobjectSyncDAO
            override val queryComposer = caseClassBObjectQueryComposer
            override val entityComposer = caseClassBObjectEntityComposer
            override val idComposer = caseClassBObjectIdComposer
        }
    }
}
