package org.beaucatcher.mongo

import org.beaucatcher.bson.ClassAnalysis
import org.beaucatcher.bson.BObject
import com.mongodb.DBObject
import org.bson.types.ObjectId

/**
 * Trait typically added to the companion object for an entity case class.
 * This trait's interface supports operations on the collection itself.
 * Here, the trait doesn't have knowledge of a specific MongoDB implementation
 * (Hammersmith, Casbah, etc.)
 */
abstract trait CollectionOperations[EntityType <: Product, IdType] {
    /**
     * This method performs any one-time-on-startup setup for the collection, such as ensuring an index.
     * The app will need to somehow arrange to call this for each collection to use this feature.
     */
    def migrate() : Unit = {}

    /** Synchronous DAO returning BObject values from the collection */
    val bobjectSyncDAO : BObjectSyncDAO[IdType]

    /** Synchronous DAO returning case class entity values from the collection */
    val caseClassSyncDAO : CaseClassSyncDAO[BObject, EntityType, IdType]

    /**
     * You have to override this from a class, because traits can't
     * put the ": Manifest" annotation on their type parameters.
     */
    protected val manifestOfEntityType : Manifest[EntityType]

    /**
     * This lets you write a function that generically works for either the case class or
     * BObject results. So for example you can implement query logic that supports
     * both kinds of result.
     */
    def syncDAO[E : Manifest] : SyncDAO[BObject, E, IdType] = {
        manifest[E] match {
            case m if m == manifest[BObject] =>
                bobjectSyncDAO.asInstanceOf[SyncDAO[BObject, E, IdType]]
            case m if m == manifestOfEntityType =>
                caseClassSyncDAO.asInstanceOf[SyncDAO[BObject, E, IdType]]
            case _ =>
                throw new IllegalArgumentException("Missing type param on syncDAO[T]? add the [T]? No DAO returns type: " + manifest[E])
        }
    }
}
