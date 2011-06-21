package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * An interface specifying a set of DAO flavors to be provided.
 * For now, the two flavors are one that returns [[org.beaucatcher.bson.BObject]] results
 * and another that returns an application-specified case class.
 */
trait SyncDAOGroup[CaseClassEntityType <: Product, CaseClassIdType, BObjectIdType] {
    /**
     *  This DAO works with a traversable immutable BSON tree (BObject), which is probably
     *  the best representation if you want to convert to JSON. You can also use
     *  the unwrappedAsJava field on BObject to get a Java map, which may work
     *  well with your HTML template system. This is intended to be the "raw"
     *  format that we'd build off the wire using Hammersmith, rather than DBObject,
     *  because it's easier to work with and immutable.
     */
    def bobjectSyncDAO : BObjectSyncDAO[BObjectIdType]

    /**
     *  This DAO works with a specified case class, for typesafe access to fields
     *  from within Scala code. You also know that all the fields are present
     *  if the case class was successfully constructed.
     */
    def caseClassSyncDAO : CaseClassSyncDAO[BObject, CaseClassEntityType, CaseClassIdType]
}
