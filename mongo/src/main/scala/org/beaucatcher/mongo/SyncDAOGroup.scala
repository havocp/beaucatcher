package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * An interface specifying a DAO objects to be provided that do not rely
 * on having an entity class (such as a case class). These DAOs are just
 * BObject-based.
 * For now there's just one, the synchronous one.
 */
private[beaucatcher] trait SyncDAOGroupWithoutEntity[BObjectIdType] {
    /**
     *  This DAO works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. You can also use
     *  the unwrappedAsJava field on BObject to get a Java map, which may work
     *  well with your HTML template system. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    def bobjectSyncDAO : BObjectSyncDAO[BObjectIdType]
}

/**
 * An interface specifying a set of DAO flavors to be provided.
 * For now, the two flavors are one that returns [[org.beaucatcher.bson.BObject]] results
 * and another that returns an application-specified case class.
 */
private[beaucatcher] trait SyncDAOGroup[EntityType <: AnyRef, EntityIdType, BObjectIdType]
    extends SyncDAOGroupWithoutEntity[BObjectIdType] {

    /**
     *  This DAO works with a specified entity class, for typesafe access to fields
     *  from within Scala code.
     */
    def entitySyncDAO : EntitySyncDAO[BObject, EntityType, EntityIdType]
}
