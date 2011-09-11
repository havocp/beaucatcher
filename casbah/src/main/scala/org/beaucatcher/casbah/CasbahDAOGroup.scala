package org.beaucatcher.casbah

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import com.mongodb.DBObject
import com.mongodb.casbah.MongoCollection

private[casbah] class InnerBValueValueComposer
    extends ValueComposer[Any, BValue] {

    import j.JavaConversions._

    override def valueIn(v : Any) : BValue = wrapJavaAsBValue(v)
    override def valueOut(v : BValue) : Any = v.unwrappedAsJava
}

private[casbah] class OuterBValueValueComposer
    extends ValueComposer[BValue, Any] {

    import j.JavaConversions._

    override def valueIn(v : BValue) : Any = v.unwrappedAsJava
    override def valueOut(v : Any) : BValue = wrapJavaAsBValue(v)
}

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
private[casbah] class EntityBObjectCasbahDAOGroup[EntityType <: AnyRef : Manifest, EntityIdType, BObjectIdType, CasbahIdType](
    val collection : MongoCollection,
    val entityBObjectQueryComposer : QueryComposer[BObject, BObject],
    val entityBObjectEntityComposer : EntityComposer[EntityType, BObject],
    val entityBObjectIdComposer : IdComposer[EntityIdType, BObjectIdType],
    private val bobjectCasbahIdComposer : IdComposer[BObjectIdType, CasbahIdType])
    extends SyncDAOGroup[EntityType, EntityIdType, BObjectIdType] {
    require(collection != null)
    require(entityBObjectQueryComposer != null)
    require(entityBObjectEntityComposer != null)
    require(entityBObjectIdComposer != null)

    /* Let's not allow changing the BObject-to-Casbah mapping since we want to
     * get rid of Casbah's DBObject. That's why these are private.
     */
    private lazy val bobjectCasbahQueryComposer : QueryComposer[BObject, DBObject] =
        new BObjectCasbahQueryComposer()
    private lazy val bobjectCasbahEntityComposer : EntityComposer[BObject, DBObject] =
        new BObjectCasbahEntityComposer()
    private lazy val bobjectCasbahValueComposer : ValueComposer[BValue, Any] =
        new OuterBValueValueComposer()

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
     *  the best representation if you want to convert to JSON. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    override lazy val bobjectSyncDAO : BObjectSyncDAO[BObjectIdType] = {
        new BObjectCasbahSyncDAO[BObjectIdType, CasbahIdType] {
            override val backend = casbahSyncDAO
            override val queryComposer = bobjectCasbahQueryComposer
            override val entityComposer = bobjectCasbahEntityComposer
            override val idComposer = bobjectCasbahIdComposer
            override val valueComposer = bobjectCasbahValueComposer
        }
    }

    /**
     *  This DAO works with a specified case class, for typesafe access to fields
     *  from within Scala code. You also know that all the fields are present
     *  if the case class was successfully constructed.
     */
    override lazy val entitySyncDAO : EntitySyncDAO[BObject, EntityType, EntityIdType] = {
        new EntityBObjectSyncDAO[EntityType, EntityIdType, BObjectIdType] {
            override val backend = bobjectSyncDAO
            override val queryComposer = entityBObjectQueryComposer
            override val entityComposer = entityBObjectEntityComposer
            override val idComposer = entityBObjectIdComposer
            override val valueComposer = new InnerBValueValueComposer()
        }
    }
}
