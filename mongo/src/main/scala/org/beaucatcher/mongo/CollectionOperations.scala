package org.beaucatcher.mongo

import org.beaucatcher.bson.ClassAnalysis
import org.beaucatcher.bson.BObject
import com.mongodb.DBObject
import org.bson.types.ObjectId
import scala.annotation.implicitNotFound

/**
 * You generally want [[org.beaucatcher.mongo.CollectionOperations]] class rather than
 * this trait; the trait exists only for the rare case where you need to extend another
 * class.
 *
 * This trait would be added to the companion object for an entity case class.
 * This trait's interface supports operations on the collection itself.
 * The trait doesn't have knowledge of a specific MongoDB implementation
 * (Hammersmith, Casbah, etc.)
 *
 * A subclass of this trait has to provide a [[org.beaucatcher.mongo.MongoBackend]] which has
 * a concrete connection to a specific MongoDB implementation.
 *
 * Implementation note: many values in this class are lazy, because otherwise class and object
 * initialization has a lot of trouble (due to circular dependencies, or order of initialization anyway).
 */
trait CollectionOperationsTrait[EntityType <: Product, IdType] {
    this : MongoBackendProvider with MongoConfigProvider =>

    import CollectionOperationsTrait._

    implicit protected def entityTypeManifest : Manifest[EntityType]

    /**
     * The name of the collection. Defaults to the unqualified (no package) name of the object,
     * with the first character made lowercase. So "object FooBar" gets collection name "fooBar" for
     * example. Override this value to change it.
     */
    val collectionName : String = {
        // "org.bar.Foo$" -> "foo"

        val fullname = getClass().getName()
        val dot = fullname.lastIndexOf('.')
        val withoutPackage = if (dot < 0) fullname else fullname.substring(dot + 1)
        // an object has a trailing $
        val withoutDollar = if (withoutPackage.endsWith("$"))
            withoutPackage.substring(0, withoutPackage.length - 1)
        else
            withoutPackage
        withoutDollar.substring(0, 1).toLowerCase + withoutDollar.substring(1)
    }

    /**
     * This method performs any one-time-on-startup setup for the collection, such as ensuring an index.
     * The app will need to somehow arrange to call this for each collection to use this feature.
     */
    def migrate() : Unit = {}

    private lazy val daoGroup : SyncDAOGroup[EntityType, IdType, IdType] = {
        require(entityTypeManifest != null)
        backend.createDAOGroup(collectionName, caseClassBObjectQueryComposer,
            caseClassBObjectEntityComposer)
    }

    /** Synchronous DAO returning BObject values from the collection */
    private[mongo] final lazy val bobjectSyncDAO : BObjectSyncDAO[IdType] =
        daoGroup.bobjectSyncDAO

    /** Synchronous DAO returning case class entity values from the collection */
    private[mongo] final lazy val caseClassSyncDAO : CaseClassSyncDAO[BObject, EntityType, IdType] =
        daoGroup.caseClassSyncDAO

    /**
     * The type of a DAO chooser that will select the proper DAO for result type E on this
     * CollectionOperations object. Used as implicit argument to syncDAO method.
     */
    type SyncDAOChooser[E] = GenericSyncDAOChooser[E, IdType, CollectionOperationsTrait[EntityType, IdType]]

    /**
     * This lets you write a function that generically works for either the case class or
     * BObject results. So for example you can implement query logic that supports
     * both kinds of result.
     */
    def syncDAO[E](implicit chooser : SyncDAOChooser[E]) : SyncDAO[BObject, E, IdType, _] = {
        chooser.choose(this)
    }

    /**
     * If the type of entity returned doesn't matter, then you can use this overload
     * of syncDAO which does not require you to specify an entity type.
     */
    def syncDAO : SyncDAO[BObject, _, IdType, _] = bobjectSyncDAO

    /**
     * There probably isn't a reason to override this, but it would modify a query
     * as it went from the case class DAO to the BObject DAO.
     */
    protected lazy val caseClassBObjectQueryComposer : QueryComposer[BObject, BObject] =
        new IdentityQueryComposer()

    /**
     * You would override this if you want to adjust how a BObject is mapped to a
     * case class entity. For example if you need to deal with missing fields or
     * database format changes, you could do that in this composer. Or if you
     * wanted to do a type mapping, say from Int to an enumeration, you could do that
     * here. Many things you might do with an annotation in something like JPA
     * could instead be done by subclassing CaseClassBObjectEntityComposer, in theory.
     */
    protected lazy val caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject] =
        new CaseClassBObjectEntityComposer[EntityType]
}

object CollectionOperationsTrait {
    // used as an implicit parameter to select the correct DAO based on requested query result type
    @implicitNotFound(msg = "No synchronous DAO that returns entity type '${E}' (with ID type '${I}' and CollectionOperations '${CO}') (implicit GenericSyncDAOChooser not resolved)")
    trait GenericSyncDAOChooser[E, I, -CO] {
        def choose(ops : CO) : SyncDAO[BObject, E, I, _]
    }

    implicit def createDAOChooserForBObject[I] : GenericSyncDAOChooser[BObject, I, CollectionOperationsTrait[_, I]] = {
        new GenericSyncDAOChooser[BObject, I, CollectionOperationsTrait[_, I]] {
            def choose(ops : CollectionOperationsTrait[_, I]) = ops.bobjectSyncDAO
        }
    }

    implicit def createDAOChooserForCaseClass[E <: Product, I] : GenericSyncDAOChooser[E, I, CollectionOperationsTrait[E, I]] = {
        new GenericSyncDAOChooser[E, I, CollectionOperationsTrait[E, I]] {
            def choose(ops : CollectionOperationsTrait[E, I]) = ops.caseClassSyncDAO
        }
    }
}

abstract class CollectionOperations[EntityType <: Product : Manifest, IdType]
    extends CollectionOperationsTrait[EntityType, IdType] {
    self : MongoBackendProvider with MongoConfigProvider =>
    override final val entityTypeManifest = manifest[EntityType]
}
