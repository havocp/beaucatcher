package com.ometer.mongo

import com.ometer.ClassAnalysis
import com.ometer.bson.BsonAST._
import com.mongodb.WriteResult

/**
 * A Sync DAO parameterized to support returning case class entities.
 */
abstract trait CaseClassSyncDAO[OuterQueryType, EntityType <: Product, IdType]
    extends SyncDAO[OuterQueryType, EntityType, IdType] {

}

/**
 * The general type of a case class DAO that backends to another DAO.
 * This is an internal implementation class not exported from the library.
 */
private[ometer] abstract trait CaseClassComposedSyncDAO[OuterQueryType, EntityType <: Product, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType]
    extends CaseClassSyncDAO[OuterQueryType, EntityType, OuterIdType]
    with ComposedSyncDAO[OuterQueryType, EntityType, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType] {
}

private[ometer] class CaseClassBObjectEntityComposer[EntityType <: Product : Manifest]
    extends EntityComposer[EntityType, BObject] {

    private lazy val analysis : ClassAnalysis[EntityType] = {
        new ClassAnalysis[EntityType](manifest[EntityType].erasure.asInstanceOf[Class[EntityType]])
    }

    override def entityIn(o : EntityType) : BObject = {
        BObject(analysis.asMap(o).map(kv => (kv._1, BValue.wrap(kv._2))))
    }

    override def entityOut(o : BObject) : EntityType = {
        analysis.fromMap(o.unwrapped)
    }
}

/**
 * A case class DAO that specifically backends to a BObject DAO and uses BObject
 * for queries.
 * Subclass would provide the backend and could override the in/out type converters.
 * This is an internal implementation class not exported from the library.
 */
private[ometer] abstract trait CaseClassBObjectSyncDAO[EntityType <: Product, OuterIdType, InnerIdType]
    extends CaseClassComposedSyncDAO[BObject, EntityType, OuterIdType, BObject, BObject, InnerIdType] {
    override protected val backend : BObjectSyncDAO[InnerIdType]

    override protected val queryComposer : QueryComposer[BObject, BObject]
    override protected val entityComposer : EntityComposer[EntityType, BObject]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
}
