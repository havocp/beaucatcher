package org.beaucatcher.hammersmith

import com.mongodb.async.Collection
import org.beaucatcher.async._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.collection._

private[hammersmith] class InnerBValueValueComposer
    extends ValueComposer[Any, BValue] {

    import j.JavaConversions._

    override def valueIn(v : Any) : BValue = wrapJavaAsBValue(v)
    override def valueOut(v : BValue) : Any = v.unwrappedAsJava
}

private[hammersmith] class OuterBValueValueComposer
    extends ValueComposer[BValue, Any] {

    import j.JavaConversions._

    override def valueIn(v : BValue) : Any = v.unwrappedAsJava
    override def valueOut(v : Any) : BValue = wrapJavaAsBValue(v)
}

private[hammersmith] class EntityBObjectHammersmithCollectionGroup[EntityType <: AnyRef : Manifest, EntityIdType, BObjectIdType, HammersmithIdType <: AnyRef](
    val collection : Collection,
    val entityBObjectQueryComposer : QueryComposer[BObject, BObject],
    val entityBObjectEntityComposer : EntityComposer[EntityType, BObject],
    val entityBObjectIdComposer : IdComposer[EntityIdType, BObjectIdType],
    private val bobjectHammersmithIdComposer : IdComposer[BObjectIdType, HammersmithIdType])
    extends SyncCollectionGroup[EntityType, EntityIdType, BObjectIdType] {
    require(collection != null)
    require(entityBObjectQueryComposer != null)
    require(entityBObjectEntityComposer != null)
    require(entityBObjectIdComposer != null)

    /* Let's not allow changing the BObject-to-Hammersmith mapping since we want to
     * get rid of Hammersmith's BSONDocument. That's why these are private.
     */
    private lazy val bobjectHammersmithQueryComposer : QueryComposer[BObject, BSONDocument] =
        new BObjectHammersmithQueryComposer()
    private lazy val bobjectHammersmithEntityComposer : EntityComposer[BObject, BObject] =
        new IdentityEntityComposer[BObject]()

    private lazy val bobjectHammersmithValueComposer : ValueComposer[BValue, Any] =
        new OuterBValueValueComposer()

    /**
     *  This is the "raw" Hammersmith Collection, if you need to work with a BSONDocument for some reason.
     *  This is best avoided because the hope is that Hammersmith would allow us to
     *  eliminate this layer. In fact, we'll make this private...
     */
    private lazy val hammersmithSyncCollection : SyncCollection[BSONDocument, BObject, HammersmithIdType, Any] = {
        makeSync(new HammersmithAsyncCollection[BObject, HammersmithIdType](collection))
    }

    override lazy val bobjectSync : BObjectSyncCollection[BObjectIdType] = {
        new BObjectHammersmithSyncCollection[BObjectIdType, HammersmithIdType] {
            override val backend = hammersmithSyncCollection
            override val queryComposer = bobjectHammersmithQueryComposer
            override val entityComposer = bobjectHammersmithEntityComposer
            override val idComposer = bobjectHammersmithIdComposer
            override val valueComposer = bobjectHammersmithValueComposer
        }
    }

    override lazy val entitySync : EntitySyncCollection[BObject, EntityType, EntityIdType] = {
        new EntityBObjectSyncCollection[EntityType, EntityIdType, BObjectIdType] {
            override val backend = bobjectSync
            override val queryComposer = entityBObjectQueryComposer
            override val entityComposer = entityBObjectEntityComposer
            override val idComposer = entityBObjectIdComposer
            override val valueComposer = new InnerBValueValueComposer()
        }
    }
}
