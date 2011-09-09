package org.beaucatcher.mongo

import org.beaucatcher.bson.ClassAnalysis
import org.beaucatcher.bson._

/**
 * A Sync DAO parameterized to support returning case class entities.
 */
abstract trait CaseClassSyncDAO[OuterQueryType, EntityType <: Product, IdType]
    extends SyncDAO[OuterQueryType, EntityType, IdType, Any] {

}

/**
 * The general type of a case class DAO that backends to another DAO.
 * This is an internal implementation class not exported from the library.
 */
private[beaucatcher] abstract trait CaseClassComposedSyncDAO[OuterQueryType, EntityType <: Product, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]
    extends CaseClassSyncDAO[OuterQueryType, EntityType, OuterIdType]
    with ComposedSyncDAO[OuterQueryType, EntityType, OuterIdType, Any, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType] {
}

private[beaucatcher] class CaseClassBObjectEntityComposer[EntityType <: Product : Manifest]
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
private[beaucatcher] abstract trait CaseClassBObjectSyncDAO[EntityType <: Product, OuterIdType, InnerIdType]
    extends CaseClassComposedSyncDAO[BObject, EntityType, OuterIdType, BObject, BObject, InnerIdType, BValue] {
    override protected val backend : BObjectSyncDAO[InnerIdType]

    override def entityToUpsertableObject(entity : EntityType) : BObject = {
        entityIn(entity)
    }

    override def entityToModifierObject(entity : EntityType) : BObject = {
        // "_id" has to come out, because you can't change the id of a document in findAndModify, update, etc.
        entityToUpsertableObject(entity) - "_id"
    }

    override def entityToUpdateQuery(entity : EntityType) : BObject = {
        // FIXME this is wasteful to copy entire entity into an object
        // then dump everything except the ID. Provide an API that does
        // something better.
        val o : BObject = entityIn(entity)
        BObject("_id" -> o.getOrElse("_id", throw new IllegalArgumentException("only objects with an _id field work here")))
    }

    override protected val queryComposer : QueryComposer[BObject, BObject]
    override protected val entityComposer : EntityComposer[EntityType, BObject]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
    override protected val valueComposer : ValueComposer[Any, BValue]
}
