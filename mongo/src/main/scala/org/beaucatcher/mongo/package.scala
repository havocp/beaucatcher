package org.beaucatcher

import org.beaucatcher.bson._

package object mongo {

    /**
     * A sync (blocking) collection parameterized to work with BObject
     */
    type BObjectSyncCollection[IdType] = SyncCollection[BObject, BObject, IdType, BValue]

    /**
     * An async (nonblocking) collection parameterized to work with BObject
     */
    type BObjectAsyncCollection[IdType] = AsyncCollection[BObject, BObject, IdType, BValue]

    /**
     * A sync (blocking) collection parameterized to support returning case class entities.
     */
    type CaseClassSyncCollection[OuterQueryType, EntityType <: Product, IdType] = EntitySyncCollection[OuterQueryType, EntityType, IdType]

    /**
     * An async (nonblocking) collection parameterized to support returning case class entities.
     */
    type CaseClassAsyncCollection[OuterQueryType, EntityType <: Product, IdType] = EntityAsyncCollection[OuterQueryType, EntityType, IdType]

    /**
     * A sync (blocking) collection parameterized to support returning some kind of domain object ("entity").
     */
    type EntitySyncCollection[OuterQueryType, EntityType <: AnyRef, IdType] = SyncCollection[OuterQueryType, EntityType, IdType, Any]

    /**
     * An async (nonblocking) collection parameterized to support returning some kind of domain object ("entity").
     */
    type EntityAsyncCollection[OuterQueryType, EntityType <: AnyRef, IdType] = AsyncCollection[OuterQueryType, EntityType, IdType, Any]
}
