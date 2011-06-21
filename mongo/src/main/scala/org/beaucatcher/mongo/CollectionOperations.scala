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
 *
 * A subclass of this trait has to provide a [[org.beaucatcher.mongo.DAOGroup]] which has
 * a concrete connection to a specific MongoDB implementation.
 */
abstract trait CollectionOperations[EntityType <: Product, IdType] {
    import CollectionOperations._

    implicit protected def entityTypeManifest : Manifest[EntityType]

    /**
     * This method performs any one-time-on-startup setup for the collection, such as ensuring an index.
     * The app will need to somehow arrange to call this for each collection to use this feature.
     */
    def migrate() : Unit = {}

    protected val daoGroup : SyncDAOGroup[EntityType, IdType, IdType]

    /** Synchronous DAO returning BObject values from the collection */
    final val bobjectSyncDAO : BObjectSyncDAO[IdType] =
        daoGroup.bobjectSyncDAO

    /** Synchronous DAO returning case class entity values from the collection */
    final val caseClassSyncDAO : CaseClassSyncDAO[BObject, EntityType, IdType] =
        daoGroup.caseClassSyncDAO

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
    def syncDAO[E](implicit chooser : SyncDAOChooser[E]) : SyncDAO[BObject, E, IdType, _] = {
        chooser.choose(this)
    }

    /**
     * There probably isn't a reason to override this, but it would modify a query
     * as it went from the case class DAO to the BObject DAO.
     */
    protected val caseClassBObjectQueryComposer : QueryComposer[BObject, BObject] =
        new IdentityQueryComposer()

    /**
     * You would override this if you want to adjust how a BObject is mapped to a
     * case class entity. For example if you need to deal with missing fields or
     * database format changes, you could do that in this composer. Or if you
     * wanted to do a type mapping, say from Int to an enumeration, you could do that
     * here. Many things you might do with an annotation in something like JPA
     * could instead be done by subclassing CaseClassBObjectEntityComposer, in theory.
     */
    protected val caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject] =
        new CaseClassBObjectEntityComposer[EntityType]
}

object CollectionOperations {
    // used as an implicit parameter to select the correct DAO based on requested query result type
    trait GenericSyncDAOChooser[E, I, -CO] {
        def choose(ops : CO) : SyncDAO[BObject, E, I, _]
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
