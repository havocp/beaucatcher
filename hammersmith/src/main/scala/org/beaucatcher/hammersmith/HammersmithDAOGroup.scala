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

private[hammersmith] class CaseClassBObjectHammersmithDAOGroup[EntityType <: Product : Manifest, CaseClassIdType, BObjectIdType, HammersmithIdType <: AnyRef](
    val collection : Collection,
    val caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
    val caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject],
    val caseClassBObjectIdComposer : IdComposer[CaseClassIdType, BObjectIdType],
    private val bobjectHammersmithIdComposer : IdComposer[BObjectIdType, HammersmithIdType])
    extends SyncDAOGroup[EntityType, CaseClassIdType, BObjectIdType] {
    require(collection != null)
    require(caseClassBObjectQueryComposer != null)
    require(caseClassBObjectEntityComposer != null)
    require(caseClassBObjectIdComposer != null)

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
     *  This is the "raw" Hammersmith DAO, if you need to work with a BSONDocument for some reason.
     *  This is best avoided because the hope is that Hammersmith would allow us to
     *  eliminate this layer. In fact, we'll make this private...
     */
    private lazy val hammersmithSyncDAO : SyncDAO[BSONDocument, BObject, HammersmithIdType, Any] = {
        makeSync(new HammersmithAsyncDAO[BObject, HammersmithIdType](collection))
    }

    override lazy val bobjectSyncDAO : BObjectSyncDAO[BObjectIdType] = {
        new BObjectHammersmithSyncDAO[BObjectIdType, HammersmithIdType] {
            override val backend = hammersmithSyncDAO
            override val queryComposer = bobjectHammersmithQueryComposer
            override val entityComposer = bobjectHammersmithEntityComposer
            override val idComposer = bobjectHammersmithIdComposer
            override val valueComposer = bobjectHammersmithValueComposer
        }
    }

    override lazy val caseClassSyncDAO : CaseClassSyncDAO[BObject, EntityType, CaseClassIdType] = {
        new CaseClassBObjectSyncDAO[EntityType, CaseClassIdType, BObjectIdType] {
            override val backend = bobjectSyncDAO
            override val queryComposer = caseClassBObjectQueryComposer
            override val entityComposer = caseClassBObjectEntityComposer
            override val idComposer = caseClassBObjectIdComposer
            override val valueComposer = new InnerBValueValueComposer()
        }
    }
}
