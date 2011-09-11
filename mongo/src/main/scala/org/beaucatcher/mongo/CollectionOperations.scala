package org.beaucatcher.mongo

import org.beaucatcher.bson._
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
trait CollectionOperationsTrait[EntityType <: AnyRef, IdType] {
    self : MongoBackendProvider with MongoConfigProvider =>

    import CollectionOperationsTrait._

    /**
     * Because traits can't have constructor arguments or context bounds, a subtype of this
     * trait has to provide the manifest for the EntityType. See [[org.beaucatcher.mongo.CollectionOperations]]
     * which is an abstract class rather than a trait, and thus implements this method.
     */
    implicit protected def entityTypeManifest : Manifest[EntityType]

    /** as with `entityTypeManifest` this is needed because traits can't have context bounds */
    implicit protected def idTypeManifest : Manifest[IdType]

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
        backend.createDAOGroup(collectionName, entityBObjectQueryComposer,
            entityBObjectEntityComposer)
    }

    /** Synchronous DAO returning BObject values from the collection */
    private[mongo] final lazy val bobjectSyncDAO : BObjectSyncDAO[IdType] =
        daoGroup.bobjectSyncDAO

    /** Synchronous DAO returning case class entity values from the collection */
    private[mongo] final lazy val entitySyncDAO : EntitySyncDAO[BObject, EntityType, IdType] =
        daoGroup.entitySyncDAO

    /**
     * The type of a DAO chooser that will select the proper DAO for result type E and value type V on this
     * CollectionOperations object. Used as implicit argument to syncDAO method.
     */
    type SyncDAOChooser[E, V] = GenericSyncDAOChooser[E, IdType, V, CollectionOperationsTrait[EntityType, IdType]]

    /**
     * This lets you write a function that generically works for either the entity (often case class) or
     * BObject results. So for example you can implement query logic that supports
     * both kinds of result.
     * {{{
     *    syncDAO[BObject].find() // returns BObject results
     *    syncDAO[MyCaseClass].find() // returns MyCaseClass results
     *    def myQuery[E] = syncDAO[E].find(... query ...) // generic query
     * }}}
     * With methods such as distinct(), you probably need the `syncDAO[E,V]` flavor that lets you specify
     * the type of field values.
     * With methods that don't return objects, such as count(), you can use the `syncDAO` flavor with no
     * type parameters.
     */
    def syncDAO[E](implicit chooser : SyncDAOChooser[E, _]) : SyncDAO[BObject, E, IdType, _] = {
        chooser.choose(this)
    }

    /**
     * This lets you specify the field value type of the synchronous DAO you are asking for;
     * the only time this matters right now is if you're using the distinct() method
     * on the DAO since it returns field values. You would use it like
     * {{{
     *    syncDAO[BObject,BValue].distinct("foo") // returns Seq[BValue]
     *    syncDAO[MyCaseClass,Any].distinct("foo") // returns Seq[Any]
     * }}}
     * Otherwise, you can use the `syncDAO[E]` version that only requires you to specify
     * the entity type, or the `syncDAO` version with no type parameters at all.
     */
    def syncDAO[E, V](implicit chooser : SyncDAOChooser[E, V], ignored : DummyImplicit) : SyncDAO[BObject, E, IdType, V] = {
        chooser.choose(this)
    }

    /**
     * If the type of entity returned doesn't matter, then you can use this overload
     * of syncDAO which does not require you to specify an entity type.
     * If you're calling a method that does return objects or field values, then you
     * need to use `syncDAO[E]` or `syncDAO[E,V]` to specify the object type or field
     * value type.
     */
    def syncDAO : SyncDAO[BObject, _, IdType, _] = bobjectSyncDAO

    /**
     * There probably isn't a reason to override this, but it would modify a query
     * as it went from the case class DAO to the BObject DAO.
     */
    protected lazy val entityBObjectQueryComposer : QueryComposer[BObject, BObject] =
        new IdentityQueryComposer()

    /**
     * You would override this if you want to adjust how a BObject is mapped to a
     * case class entity. For example if you need to deal with missing fields or
     * database format changes, you could do that in this composer. Or if you
     * wanted to do a type mapping, say from Int to an enumeration, you could do that
     * here. Many things you might do with an annotation in something like JPA
     * could instead be done by subclassing CaseClassBObjectEntityComposer, in theory.
     */
    protected def entityBObjectEntityComposer : EntityComposer[EntityType, BObject]
}

object CollectionOperationsTrait {
    // used as an implicit parameter to select the correct DAO based on requested query result type
    @implicitNotFound(msg = "No synchronous DAO that returns entity type '${E}' (with ID type '${I}', value type '${V}', CollectionOperations '${CO}') (implicit GenericSyncDAOChooser not resolved) (note: scala 2.9.0 seems to confuse the id type with value type in this message)")
    trait GenericSyncDAOChooser[E, I, V, -CO] {
        def choose(ops : CO) : SyncDAO[BObject, E, I, V]
    }

    implicit def createDAOChooserForBObject[I] : GenericSyncDAOChooser[BObject, I, BValue, CollectionOperationsTrait[_, I]] = {
        new GenericSyncDAOChooser[BObject, I, BValue, CollectionOperationsTrait[_, I]] {
            def choose(ops : CollectionOperationsTrait[_, I]) = ops.bobjectSyncDAO
        }
    }

    implicit def createDAOChooserForEntity[E <: AnyRef, I] : GenericSyncDAOChooser[E, I, Any, CollectionOperationsTrait[E, I]] = {
        new GenericSyncDAOChooser[E, I, Any, CollectionOperationsTrait[E, I]] {
            def choose(ops : CollectionOperationsTrait[E, I]) = ops.entitySyncDAO
        }
    }
}

trait CollectionOperationsWithCaseClassTrait[EntityType <: Product, IdType]
    extends CollectionOperationsTrait[EntityType, IdType] {
    self : MongoBackendProvider with MongoConfigProvider =>

    override protected lazy val entityBObjectEntityComposer : EntityComposer[EntityType, BObject] =
        new CaseClassBObjectEntityComposer[EntityType]
}

abstract class CollectionOperations[EntityType <: AnyRef : Manifest, IdType : Manifest]
    extends CollectionOperationsTrait[EntityType, IdType] {
    self : MongoBackendProvider with MongoConfigProvider =>
    override final val entityTypeManifest = manifest[EntityType]
    override final val idTypeManifest = manifest[IdType]
}

abstract class CollectionOperationsWithCaseClass[EntityType <: Product : Manifest, IdType : Manifest]
    extends CollectionOperations[EntityType, IdType]
    with CollectionOperationsWithCaseClassTrait[EntityType, IdType] {
    self : MongoBackendProvider with MongoConfigProvider =>

}
