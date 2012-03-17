package org.beaucatcher.jdriver

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import com.mongodb.DBObject
import com.mongodb.DBCollection
import akka.actor.ActorSystem

private[jdriver] class InnerBValueValueComposer
    extends ValueComposer[Any, BValue] {

    import JavaConversions._

    override def valueIn(v : Any) : BValue = wrapJavaAsBValue(v)
    override def valueOut(v : BValue) : Any = v.unwrappedAsJava
}

private[jdriver] class OuterBValueValueComposer
    extends ValueComposer[BValue, Any] {

    import JavaConversions._

    override def valueIn(v : BValue) : Any = v.unwrappedAsJava
    override def valueOut(v : Any) : BValue = wrapJavaAsBValue(v)
}

private[jdriver] class BObjectJavaDriverCollectionGroup[BObjectIdType, JavaDriverIdType](
    val backend : JavaDriverBackend,
    val collection : DBCollection,
    private val bobjectJavaDriverIdComposer : IdComposer[BObjectIdType, JavaDriverIdType])
    extends CollectionGroupWithoutEntity[BObjectIdType] {
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
     *  This is the "raw" JavaDriver Collection, if you need to work with a DBObject for some reason.
     *  This is best avoided because the hope is that Hammersmith would allow us to
     *  eliminate this layer. In fact, we'll make this private...
     */
    private lazy val jdriverSyncCollection : JavaDriverSyncCollection[JavaDriverIdType] = {
        val outerCollection = collection
        val outerBackend = backend
        new JavaDriverSyncCollection[JavaDriverIdType] {
            override val collection = outerCollection
            override val backend = outerBackend
        }
    }

    /**
     *  This Collection works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    override def newBObjectSync : BObjectSyncCollection[BObjectIdType] = {
        val outerBackend = backend
        new BObjectJavaDriverSyncCollection[BObjectIdType, JavaDriverIdType] {
            override val inner = jdriverSyncCollection
            override val backend = outerBackend
            override val queryComposer = bobjectJavaDriverQueryComposer
            override val entityComposer = bobjectJavaDriverEntityComposer
            override val idComposer = bobjectJavaDriverIdComposer
            override val valueComposer = bobjectJavaDriverValueComposer
        }
    }

    /**
     *  This Collection works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    override def newBObjectAsync(implicit system : ActorSystem) : BObjectAsyncCollection[BObjectIdType] =
        AsyncCollection.fromSync(newBObjectSync)
}

/**
 * A CollectionGroup exposes the entire chain of Collection conversions; you can "tap in" and use the
 * nicest Collection at whatever level is convenient for whatever you're doing. You can also
 * override any of the conversions as the data makes its way up from MongoDB.
 *
 * The long-term idea is to get rid of the JavaDriver part, and the CollectionGroup will
 * have a 2x2 of Collection flavors: sync vs. async, and BObject vs. case entity.
 *
 * Rather than a bunch of annotations specifying how to go from MongoDB to
 * the case entity, there's a theory here that you can override the
 * "composer" objects and do things like validation or dealing with legacy
 * object formats in there.
 */
private[jdriver] class EntityBObjectJavaDriverCollectionGroup[EntityType <: AnyRef : Manifest, EntityIdType, BObjectIdType, JavaDriverIdType](
    override val backend : JavaDriverBackend,
    override val collection : DBCollection,
    val entityBObjectQueryComposer : QueryComposer[BObject, BObject],
    val entityBObjectEntityComposer : EntityComposer[EntityType, BObject],
    val entityBObjectIdComposer : IdComposer[EntityIdType, BObjectIdType],
    private val bobjectJavaDriverIdComposer : IdComposer[BObjectIdType, JavaDriverIdType])
    extends BObjectJavaDriverCollectionGroup[BObjectIdType, JavaDriverIdType](backend, collection, bobjectJavaDriverIdComposer)
    with CollectionGroup[EntityType, EntityIdType, BObjectIdType] {
    require(backend != null)
    require(collection != null)
    require(entityBObjectQueryComposer != null)
    require(entityBObjectEntityComposer != null)
    require(entityBObjectIdComposer != null)

    /**
     *  This Collection works with a specified case class, for typesafe access to fields
     *  from within Scala code. You also know that all the fields are present
     *  if the case class was successfully constructed.
     */
    override def newEntitySync : EntitySyncCollection[BObject, EntityType, EntityIdType] = {
        val outerBackend = backend
        new EntityBObjectSyncCollection[EntityType, EntityIdType, BObjectIdType] {
            override val inner = newBObjectSync
            override val backend = outerBackend
            override val queryComposer = entityBObjectQueryComposer
            override val entityComposer = entityBObjectEntityComposer
            override val idComposer = entityBObjectIdComposer
            override val valueComposer = new InnerBValueValueComposer()
        }
    }

    /**
     *  This Collection works with a specified case class, for typesafe access to fields
     *  from within Scala code. You also know that all the fields are present
     *  if the case class was successfully constructed.
     */
    override def newEntityAsync(implicit system : ActorSystem) : EntityAsyncCollection[BObject, EntityType, EntityIdType] = {
        AsyncCollection.fromSync(newEntitySync)
    }
}
