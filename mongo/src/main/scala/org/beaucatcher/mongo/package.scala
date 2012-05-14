package org.beaucatcher

import org.beaucatcher.bson._

package object mongo {

    /**
     * A sync (blocking) collection parameterized to work with BObject
     */
    type BObjectSyncCollection[IdType] = BoundSyncCollection[BObject, BObject, IdType, BValue]

    /**
     * An async (nonblocking) collection parameterized to work with BObject
     */
    type BObjectAsyncCollection[IdType] = BoundAsyncCollection[BObject, BObject, IdType, BValue]

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
    type EntitySyncCollection[OuterQueryType, EntityType <: AnyRef, IdType] = BoundSyncCollection[OuterQueryType, EntityType, IdType, Any]

    /**
     * An async (nonblocking) collection parameterized to support returning some kind of domain object ("entity").
     */
    type EntityAsyncCollection[OuterQueryType, EntityType <: AnyRef, IdType] = BoundAsyncCollection[OuterQueryType, EntityType, IdType, Any]

    private[mongo] object AssertNotUsedEncoder
        extends QueryEncoder[Any]
        with UpsertEncoder[Any]
        with ModifierEncoder[Any] {
        private def unused(): Exception =
            new BugInSomethingMongoException("encoder should not have been used")

        override def encode(buf: EncodeBuffer, t: Any): Unit = throw unused()
        override def encodeIterator(t: Any): Iterator[(String, Any)] = throw unused()
    }
}
