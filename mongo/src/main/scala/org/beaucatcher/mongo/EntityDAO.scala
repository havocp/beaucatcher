package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * A Sync DAO parameterized to support returning some kind of domain object ("entity").
 */
abstract trait EntitySyncDAO[OuterQueryType, EntityType <: AnyRef, IdType]
    extends SyncDAO[OuterQueryType, EntityType, IdType, Any] {

}

/**
 * The general type of an entity DAO that backends to another DAO.
 * This is an internal implementation class not exported from the library.
 */
private[beaucatcher] abstract trait EntityComposedSyncDAO[OuterQueryType, EntityType <: AnyRef, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]
    extends EntitySyncDAO[OuterQueryType, EntityType, OuterIdType]
    with ComposedSyncDAO[OuterQueryType, EntityType, OuterIdType, Any, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType] {
}

/**
 * An entity DAO that specifically backends to a BObject DAO and uses BObject
 * for queries.
 * Subclass would provide the backend and could override the in/out type converters.
 * This is an internal implementation class not exported from the library.
 */
private[beaucatcher] abstract trait EntityBObjectSyncDAO[EntityType <: AnyRef, OuterIdType, InnerIdType]
    extends EntityComposedSyncDAO[BObject, EntityType, OuterIdType, BObject, BObject, InnerIdType, BValue] {
    override protected val inner : BObjectSyncDAO[InnerIdType]

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
