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
    import CollectionOperations._

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
     * The type of a DAO chooser that will select the proper DAO for result type E on this
     * CollectionOperations object. Used as implicit argument to syncDAO method.
     */
    type SyncDAOChooser[E] = GenericSyncDAOChooser[E, IdType, CollectionOperations[EntityType, IdType]]

    /**
     * This lets you write a function that generically works for either the case class or
     * BObject results. So for example you can implement query logic that supports
     * both kinds of result.
     */
    def syncDAO[E](implicit chooser : SyncDAOChooser[E]) : SyncDAO[BObject, E, IdType] = {
        chooser.choose(this)
    }
}

object CollectionOperations {
    // used as an implicit parameter to select the correct DAO based on requested query result type
    trait GenericSyncDAOChooser[E, I, -CO] {
        def choose(ops : CO) : SyncDAO[BObject, E, I]
    }

    implicit def createDAOChooserForBObject[I] : GenericSyncDAOChooser[BObject, I, CollectionOperations[_, I]] = {
        new GenericSyncDAOChooser[BObject, I, CollectionOperations[_, I]] {
            def choose(ops : CollectionOperations[_, I]) = ops.bobjectSyncDAO
        }
    }

    implicit def createDAOChooserForCaseClass[E <: Product, I] : GenericSyncDAOChooser[E, I, CollectionOperations[E, I]] = {
        new GenericSyncDAOChooser[E, I, CollectionOperations[E, I]] {
            def choose(ops : CollectionOperations[E, I]) = ops.caseClassSyncDAO
        }
    }
}
