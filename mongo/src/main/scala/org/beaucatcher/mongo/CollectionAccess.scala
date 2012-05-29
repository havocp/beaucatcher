package org.beaucatcher.mongo

import org.beaucatcher.bson._
import scala.annotation.implicitNotFound

/** This is provided as an implicit, its purpose is to provide a codec set suitable for a given CollectionAccess. */
@implicitNotFound("Unable to find a CodecSetProvider for result type ${DecodeEntityType}, value type ${ValueType}, access object ${CollectionAccessType}")
trait CodecSetProvider[+DecodeEntityType, ValueType, -CollectionAccessType, -CAQueryType, -CAEncodeEntityType, -CAIdType] {
    def codecSet(access: CollectionAccessType): CollectionCodecSet[CAQueryType, CAEncodeEntityType, DecodeEntityType, CAIdType, ValueType]
}

object CodecSetProvider {

}

trait CollectionAccessLike[QueryType, IdType, +Repr] {
    self: Repr =>

    /**
     * The name of the collection. Defaults to the unqualified (no package) name of the object,
     * with the first character made lowercase. So "object FooBar" gets collection name "fooBar" for
     * example. Override this value to change it.
     */
    def collectionName: String = defaultCollectionName

    private lazy val defaultCollectionName: String = {
        // FIXME getClass.getSimpleName ?
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
     * It's automatically called once per [[org.beaucatcher.mongo.Context]].
     * Be sure to block (use the sync collections) so the migration is complete when this method returns.
     */
    def migrate(implicit context: Context): Unit = {}

    private[mongo] val migrator = new Migrator()
    private[mongo] val migratorFunction: (Context) => Unit = migrate(_)

    // this needs to be called before returning any collection
    private[mongo] def ensureMigrated(context: Context): Unit = {
        migrator.ensureMigrated(context, collectionName, migratorFunction)
    }

    private lazy val unboundSyncCache = ContextCache { implicit context =>
        ensureMigrated(context)
        SyncCollection(collectionName)
    }

    private lazy val unboundAsyncCache = ContextCache { implicit context =>
        ensureMigrated(context)
        AsyncCollection(collectionName)
    }

    def syncUnbound(implicit context: Context): SyncCollection = {
        unboundSyncCache.get
    }

    def sync[E](implicit context: Context, provider: CodecSetProvider[E, ErrorIfDecodedValue, Repr, QueryType, E, IdType]): BoundSyncCollection[QueryType, E, E, IdType, ErrorIfDecodedValue] = {
        implicit val codecs = provider.codecSet(this)
        BoundSyncCollection(syncUnbound)
    }

    def sync[E, V](implicit context: Context, provider: CodecSetProvider[E, V, Repr, QueryType, E, IdType], ignored: DummyImplicit): BoundSyncCollection[QueryType, E, E, IdType, V] = {
        implicit val codecs = provider.codecSet(this)
        BoundSyncCollection(syncUnbound)
    }

    def asyncUnbound(implicit context: Context): AsyncCollection = {
        unboundAsyncCache.get
    }

    def async[E](implicit context: Context, provider: CodecSetProvider[E, ErrorIfDecodedValue, Repr, QueryType, E, IdType]): BoundAsyncCollection[QueryType, E, E, IdType, ErrorIfDecodedValue] = {
        implicit val codecs = provider.codecSet(this)
        BoundAsyncCollection(asyncUnbound)
    }

    def async[E, V](implicit context: Context, provider: CodecSetProvider[E, V, Repr, QueryType, E, IdType], ignored: DummyImplicit): BoundAsyncCollection[QueryType, E, E, IdType, V] = {
        implicit val codecs = provider.codecSet(this)
        BoundAsyncCollection(asyncUnbound)
    }
}

trait CollectionAccess[QueryType, IdType]
    extends CollectionAccessLike[QueryType, IdType, CollectionAccess[QueryType, IdType]] {

}

// TODO support Map entity

trait CollectionAccessWithFirstCodecSet[-QueryType, -EncodeEntityType, +DecodeEntityType, -IdType, +ValueType] {
    def firstCodecSet: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]
}

trait CollectionAccessWithSecondCodecSet[-QueryType, -EncodeEntityType, +DecodeEntityType, -IdType, +ValueType] {
    def secondCodecSet: CollectionCodecSet[QueryType, EncodeEntityType, DecodeEntityType, IdType, ValueType]
}

trait CollectionAccessWithOneEntityType[QueryType, EntityType, IdType, +ValueType]
    extends CollectionAccess[QueryType, IdType]
    with CollectionAccessWithFirstCodecSet[QueryType, EntityType, EntityType, IdType, ValueType]
    with CollectionAccessLike[QueryType, IdType, CollectionAccessWithOneEntityType[QueryType, EntityType, IdType, ValueType]] {

    override def firstCodecSet: CollectionCodecSet[QueryType, EntityType, EntityType, IdType, ValueType]

    def sync(implicit context: Context): BoundSyncCollection[QueryType, EntityType, EntityType, IdType, ValueType] = {
        BoundSyncCollection(syncUnbound)(firstCodecSet)
    }

    def async(implicit context: Context): BoundAsyncCollection[QueryType, EntityType, EntityType, IdType, ValueType] = {
        BoundAsyncCollection(asyncUnbound)(firstCodecSet)
    }

}

object CollectionAccessWithOneEntityType {
    def apply[QueryType, EntityType, IdType, ValueType](name: String,
        migrateCallback: (CollectionAccessWithOneEntityType[QueryType, EntityType, IdType, ValueType], Context) => Unit)(implicit codecSet: CollectionCodecSet[QueryType, EntityType, EntityType, IdType, ValueType]) = {
        new CollectionAccessWithOneEntityType[QueryType, EntityType, IdType, ValueType]() {
            override val collectionName = name

            override val firstCodecSet = codecSet

            override def migrate(implicit context: Context) = migrateCallback(this, context)
        }
    }

    implicit def firstCodecSetProviderE[CAQueryType, DecodeEntityType, CAIdType, CAValueType, CA <: CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, CAValueType]]: CodecSetProvider[DecodeEntityType, ErrorIfDecodedValue, CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, CAValueType], CAQueryType, DecodeEntityType, CAIdType] = {
        new CodecSetProvider[DecodeEntityType, ErrorIfDecodedValue, CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, CAValueType], CAQueryType, DecodeEntityType, CAIdType]() {
            override def codecSet(access: CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, CAValueType]) =
                access.firstCodecSet.toErrorIfValueDecoded
        }
    }

    implicit def firstCodecSetProviderEV[CAQueryType, DecodeEntityType, CAIdType, ValueType, CA <: CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, ValueType]]: CodecSetProvider[DecodeEntityType, ValueType, CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, ValueType], CAQueryType, DecodeEntityType, CAIdType] = {
        new CodecSetProvider[DecodeEntityType, ValueType, CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, ValueType], CAQueryType, DecodeEntityType, CAIdType]() {
            override def codecSet(access: CollectionAccessWithOneEntityType[CAQueryType, DecodeEntityType, CAIdType, ValueType]) =
                access.firstCodecSet
        }
    }
}

trait CollectionAccessWithTwoEntityTypes[QueryType, IdType, EntityType1, +ValueType1, EntityType2, +ValueType2]
    extends CollectionAccess[QueryType, IdType]
    with CollectionAccessWithFirstCodecSet[QueryType, EntityType1, EntityType1, IdType, ValueType1]
    with CollectionAccessWithSecondCodecSet[QueryType, EntityType2, EntityType2, IdType, ValueType2]
    with CollectionAccessLike[QueryType, IdType, CollectionAccessWithTwoEntityTypes[QueryType, IdType, EntityType1, ValueType1, EntityType2, ValueType2]] {

    override def firstCodecSet: CollectionCodecSet[QueryType, EntityType1, EntityType1, IdType, ValueType1]
    override def secondCodecSet: CollectionCodecSet[QueryType, EntityType2, EntityType2, IdType, ValueType2]

    def sync(implicit context: Context): BoundSyncCollection[QueryType, EntityType1, EntityType1, IdType, ValueType1] = {
        BoundSyncCollection(syncUnbound)(firstCodecSet)
    }

    def async(implicit context: Context): BoundAsyncCollection[QueryType, EntityType1, EntityType1, IdType, ValueType1] = {
        BoundAsyncCollection(asyncUnbound)(firstCodecSet)
    }
}

object CollectionAccessWithTwoEntityTypes {
    def apply[QueryType, IdType, EntityType1, ValueType1, EntityType2, ValueType2](name: String,
        migrateCallback: (CollectionAccessWithTwoEntityTypes[QueryType, IdType, EntityType1, ValueType1, EntityType2, ValueType2], Context) => Unit)(implicit codecSetOne: CollectionCodecSet[QueryType, EntityType1, EntityType1, IdType, ValueType1],
            codecSetTwo: CollectionCodecSet[QueryType, EntityType2, EntityType2, IdType, ValueType2]) = {
        new CollectionAccessWithTwoEntityTypes[QueryType, IdType, EntityType1, ValueType1, EntityType2, ValueType2] {
            override val collectionName = name

            override val firstCodecSet = codecSetOne
            override val secondCodecSet = codecSetTwo

            override def migrate(implicit context: Context) = migrateCallback(this, context)
        }
    }

    implicit def firstCodecSetProviderE[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2, CA <: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2]]: CodecSetProvider[DecodeEntityType, ErrorIfDecodedValue, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2], CAQueryType, DecodeEntityType, CAIdType] = {
        new CodecSetProvider[DecodeEntityType, ErrorIfDecodedValue, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2], CAQueryType, DecodeEntityType, CAIdType]() {
            override def codecSet(access: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2]) =
                access.firstCodecSet.toErrorIfValueDecoded
        }
    }

    implicit def firstCodecSetProviderEV[CAQueryType, CAIdType, DecodeEntityType, ValueType, CAEntityType, CAValueType, CA <: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, ValueType, CAEntityType, CAValueType]]: CodecSetProvider[DecodeEntityType, ValueType, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, ValueType, CAEntityType, CAValueType], CAQueryType, DecodeEntityType, CAIdType] = {
        new CodecSetProvider[DecodeEntityType, ValueType, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, ValueType, CAEntityType, CAValueType], CAQueryType, DecodeEntityType, CAIdType]() {
            override def codecSet(access: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, ValueType, CAEntityType, CAValueType]) =
                access.firstCodecSet
        }
    }

    implicit def secondCodecSetProviderE[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2, CA <: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, DecodeEntityType, CAValueType1, CAEntityType, CAValueType2]]: CodecSetProvider[DecodeEntityType, ErrorIfDecodedValue, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType1, DecodeEntityType, CAValueType2], CAQueryType, DecodeEntityType, CAIdType] = {
        new CodecSetProvider[DecodeEntityType, ErrorIfDecodedValue, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType1, DecodeEntityType, CAValueType2], CAQueryType, DecodeEntityType, CAIdType]() {
            override def codecSet(access: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType1, DecodeEntityType, CAValueType2]) =
                access.secondCodecSet.toErrorIfValueDecoded
        }
    }

    implicit def secondCodecSetProviderEV[CAQueryType, CAIdType, DecodeEntityType, ValueType, CAEntityType, CAValueType, CA <: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType, DecodeEntityType, ValueType]]: CodecSetProvider[DecodeEntityType, ValueType, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType, DecodeEntityType, ValueType], CAQueryType, DecodeEntityType, CAIdType] = {
        new CodecSetProvider[DecodeEntityType, ValueType, CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType, DecodeEntityType, ValueType], CAQueryType, DecodeEntityType, CAIdType]() {
            override def codecSet(access: CollectionAccessWithTwoEntityTypes[CAQueryType, CAIdType, CAEntityType, CAValueType, DecodeEntityType, ValueType]) =
                access.secondCodecSet
        }
    }
}
