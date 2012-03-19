package org.beaucatcher.mongo

import org.beaucatcher.bson._
import akka.actor.ActorSystem

/**
 * An internal interface specifying a Collection objects to be provided that do not rely
 * on having an entity class (such as a case class). These Collections are just
 * BObject-based.
 *
 * This interface is provided by an instance of [[org.beaucatcher.Driver]].
 */
private[beaucatcher] trait CollectionFactoryWithoutEntity[BObjectIdType] {
    /**
     *  This Collection works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. You can also use
     *  the unwrappedAsJava field on BObject to get a Java map, which may work
     *  well with your HTML template system. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    def newBObjectSync(implicit context : Context) : BObjectSyncCollection[BObjectIdType]

    def newBObjectAsync(implicit context : Context) : BObjectAsyncCollection[BObjectIdType]
}

/**
 * An internal interface specifying a set of Collection flavors to be provided.
 * For now, the two flavors are one that returns [[org.beaucatcher.bson.BObject]] results
 * and another that returns an application-specified case class.
 *
 * This interface is provided by an instance of [[org.beaucatcher.Driver]].
 */
private[beaucatcher] trait CollectionFactory[EntityType <: AnyRef, EntityIdType, BObjectIdType]
    extends CollectionFactoryWithoutEntity[BObjectIdType] {

    /**
     *  This Collection works with a specified entity class, for typesafe access to fields
     *  from within Scala code.
     */
    def newEntitySync(implicit context : Context) : EntitySyncCollection[BObject, EntityType, EntityIdType]

    def newEntityAsync(implicit context : Context) : EntityAsyncCollection[BObject, EntityType, EntityIdType]
}
