package org.beaucatcher.jdriver

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import com.mongodb.DBObject
import com.mongodb.DBCollection

private[jdriver] class InnerBValueValueComposer
    extends ValueComposer[Any, BValue] {

    import j.JavaConversions._

    override def valueIn(v : Any) : BValue = wrapJavaAsBValue(v)
    override def valueOut(v : BValue) : Any = v.unwrappedAsJava
}

private[jdriver] class OuterBValueValueComposer
    extends ValueComposer[BValue, Any] {

    import j.JavaConversions._

    override def valueIn(v : BValue) : Any = v.unwrappedAsJava
    override def valueOut(v : Any) : BValue = wrapJavaAsBValue(v)
}

private[jdriver] class BObjectJavaDriverDAOGroup[BObjectIdType, JavaDriverIdType](
    val backend : JavaDriverBackend,
    val collection : DBCollection,
    private val bobjectJavaDriverIdComposer : IdComposer[BObjectIdType, JavaDriverIdType])
    extends SyncDAOGroupWithoutEntity[BObjectIdType] {
    require(backend != null)
    require(collection != null)

    /* Let's not allow changing the BObject-to-JavaDriver mapping since we want to
     * get rid of JavaDriver's DBObject. That's why these are private.
     */
    private lazy val bobjectJavaDriverQueryComposer : QueryComposer[BObject, DBObject] =
        new BObjectJavaDriverQueryComposer()
    private lazy val bobjectJavaDriverEntityComposer : EntityComposer[BObject, DBObject] =
        new BObjectJavaDriverEntityComposer()
    private lazy val bobjectJavaDriverValueComposer : ValueComposer[BValue, Any] =
        new OuterBValueValueComposer()

    /**
     *  This is the "raw" JavaDriver DAO, if you need to work with a DBObject for some reason.
     *  This is best avoided because the hope is that Hammersmith would allow us to
     *  eliminate this layer. In fact, we'll make this private...
     */
    private lazy val jdriverSyncDAO : JavaDriverSyncDAO[JavaDriverIdType] = {
        val outerCollection = collection
        val outerBackend = backend
        new JavaDriverSyncDAO[JavaDriverIdType] {
            override val collection = outerCollection
            override val backend = outerBackend
        }
    }

    /**
     *  This DAO works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    override lazy val bobjectSync : BObjectSyncDAO[BObjectIdType] = {
        val outerBackend = backend
        new BObjectJavaDriverSyncDAO[BObjectIdType, JavaDriverIdType] {
            override val inner = jdriverSyncDAO
            override val backend = outerBackend
            override val queryComposer = bobjectJavaDriverQueryComposer
            override val entityComposer = bobjectJavaDriverEntityComposer
            override val idComposer = bobjectJavaDriverIdComposer
            override val valueComposer = bobjectJavaDriverValueComposer
        }
    }
}

/**
 * A DAOGroup exposes the entire chain of DAO conversions; you can "tap in" and use the
 * nicest DAO at whatever level is convenient for whatever you're doing. You can also
 * override any of the conversions as the data makes its way up from MongoDB.
 *
 * The long-term idea is to get rid of the JavaDriver part, and the DAOGroup will
 * have a 2x2 of DAO flavors: sync vs. async, and BObject vs. case entity.
 *
 * Rather than a bunch of annotations specifying how to go from MongoDB to
 * the case entity, there's a theory here that you can override the
 * "composer" objects and do things like validation or dealing with legacy
 * object formats in there.
 */
private[jdriver] class EntityBObjectJavaDriverDAOGroup[EntityType <: AnyRef : Manifest, EntityIdType, BObjectIdType, JavaDriverIdType](
    override val backend : JavaDriverBackend,
    override val collection : DBCollection,
    val entityBObjectQueryComposer : QueryComposer[BObject, BObject],
    val entityBObjectEntityComposer : EntityComposer[EntityType, BObject],
    val entityBObjectIdComposer : IdComposer[EntityIdType, BObjectIdType],
    private val bobjectJavaDriverIdComposer : IdComposer[BObjectIdType, JavaDriverIdType])
    extends BObjectJavaDriverDAOGroup[BObjectIdType, JavaDriverIdType](backend, collection, bobjectJavaDriverIdComposer)
    with SyncDAOGroup[EntityType, EntityIdType, BObjectIdType] {
    require(backend != null)
    require(collection != null)
    require(entityBObjectQueryComposer != null)
    require(entityBObjectEntityComposer != null)
    require(entityBObjectIdComposer != null)

    /**
     *  This DAO works with a specified case class, for typesafe access to fields
     *  from within Scala code. You also know that all the fields are present
     *  if the case class was successfully constructed.
     */
    override lazy val entitySync : EntitySyncDAO[BObject, EntityType, EntityIdType] = {
        val outerBackend = backend
        new EntityBObjectSyncDAO[EntityType, EntityIdType, BObjectIdType] {
            override val inner = bobjectSync
            override val backend = outerBackend
            override val queryComposer = entityBObjectQueryComposer
            override val entityComposer = entityBObjectEntityComposer
            override val idComposer = entityBObjectIdComposer
            override val valueComposer = new InnerBValueValueComposer()
        }
    }
}
