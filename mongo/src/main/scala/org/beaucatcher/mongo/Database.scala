package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.driver._
import akka.dispatch.Future

/**
 * Trait contains [[org.beaucatcher.mongo.CollectionAccess]] for some of mongo's
 * built-in collections. Obtain it through a [[org.beaucatcher.mongo.Database]] object,
 * you can't construct a [[org.beaucatcher.mongo.SystemCollections]] directly.
 */
final class SystemCollections private[mongo] () {
    import IdEncoders._

    private abstract class CollectionAccessWithEntityCaseClass[EntityType <: Product, IdType]()(implicit entityManifest: Manifest[EntityType], idEncoder: IdEncoder[IdType])
        extends CollectionAccessWithOneEntityType[Iterator[(String, Any)], EntityType, IdType, Any] {

        override val firstCodecSet = CollectionCodecSetCaseClass[Iterator[(String, Any)], EntityType, IdType]()(entityManifest, IteratorCodecs.iteratorQueryEncoder, IteratorCodecs.iteratorModifierEncoder, idEncoder)
    }

    private object Indexes
        extends CollectionAccessWithEntityCaseClass[CollectionIndex, String] {
        override val collectionName = "system.indexes"
    }

    def indexes: CollectionAccessWithOneEntityType[Iterator[(String, Any)], CollectionIndex, String, Any] = Indexes

    private object Namespaces
        extends CollectionAccessWithEntityCaseClass[Namespace, String] {
        override val collectionName = "system.namespaces"
    }

    def namespaces: CollectionAccessWithOneEntityType[Iterator[(String, Any)], Namespace, String, Any] = Namespaces

    // TODO system.users
}

case class CreateCollectionOptions(autoIndexId: Option[Boolean] = None, capped: Option[Boolean] = None,
    max: Option[Int] = None, size: Option[Int] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None)

private[beaucatcher] object CreateCollectionOptions {
    final val empty = CreateCollectionOptions(None, None, None, None, None)

    private[beaucatcher] def buildCommand(name: String, options: CreateCollectionOptions): Iterator[(String, Any)] = {
        val builder = Seq.newBuilder[(String, Any)]
        // the Mongo docs say "createCollection" but driver uses "create"
        builder += ("create" -> name)
        // use "match" here so it will break if we modify CreateCollectionOptions fields
        options match {
            case CreateCollectionOptions(autoIndexId, capped, max, size, overrideQueryFlags) =>
                autoIndexId foreach { v => builder += ("autoIndexId" -> v) }
                capped foreach { v => builder += ("capped" -> v) }
                max foreach { v => builder += ("max" -> v) }
                size foreach { v => builder += ("size" -> v) }
                builder.result.iterator
            case _ =>
                throw new IllegalStateException("should not be reached")
        }
    }
}

trait SyncDatabase {
    protected[mongo] def database: Database

    private implicit def context: Context = database.context

    private lazy val underlying = context.driverContext.database.sync

    // TODO authenticate()
    // TODO addUser(), removeUser()
    // TODO eval()

    final def command[Q](cmd: Q)(implicit encoder: QueryEncoder[Q]): CommandResult = command(cmd, CommandOptions.empty)

    def command[Q](cmd: Q, options: CommandOptions)(implicit encoder: QueryEncoder[Q]): CommandResult = {
        import BObjectCodecs._
        underlying.command(cmd, options)
    }

    final def createCollection(name: String): CommandResult = {
        createCollection(name, CreateCollectionOptions.empty)
    }

    final def createCollection(name: String, options: CreateCollectionOptions): CommandResult = {
        import IteratorCodecs.iteratorQueryEncoder
        command(CreateCollectionOptions.buildCommand(name, options), CommandOptions(options.overrideQueryFlags))
    }

    def collectionNames: Cursor[String] = {
        database.system.namespaces.sync.find(Iterator.empty, IncludedFields("name")).map(_.name)
    }

    def dropDatabase(): CommandResult = {
        import IteratorCodecs.iteratorQueryEncoder
        command(Iterator("dropDatabase" -> 1))
    }
}

trait AsyncDatabase {
    protected[mongo] def database: Database

    private implicit def context: Context = database.context

    private lazy val underlying = context.driverContext.database.async

    // TODO authenticate()
    // TODO addUser(), removeUser()
    // TODO eval()

    final def command[Q](cmd: Q)(implicit encoder: QueryEncoder[Q]): Future[CommandResult] = command(cmd, CommandOptions.empty)

    def command[Q](cmd: Q, options: CommandOptions)(implicit encoder: QueryEncoder[Q]): Future[CommandResult] = {
        underlying.command(cmd, options)
    }

    final def createCollection(name: String): Future[CommandResult] = {
        createCollection(name, CreateCollectionOptions.empty)
    }

    final def createCollection(name: String, options: CreateCollectionOptions): Future[CommandResult] = {
        import IteratorCodecs.iteratorQueryEncoder
        command(CreateCollectionOptions.buildCommand(name, options), CommandOptions(options.overrideQueryFlags))
    }

    def collectionNames: Future[AsyncCursor[String]] = {
        database.system.namespaces.async.find(Iterator.empty, IncludedFields("name")) map { cursor =>
            cursor.map(_.name)
        }
    }

    def dropDatabase(): Future[CommandResult] = {
        import IteratorCodecs.iteratorQueryEncoder
        command(Iterator("dropDatabase" -> 1))
    }
}

trait Database {
    def context: Context

    def name: String

    def system: SystemCollections = Database.systemCollections

    def sync: SyncDatabase

    def async: AsyncDatabase
}

object Database {
    private class SyncImpl(override val database: Database) extends SyncDatabase
    private class AsyncImpl(override val database: Database) extends AsyncDatabase

    private class DatabaseImpl(override val context: Context) extends Database {
        val underlying = context.driverContext.database
        override def name = underlying.name
        override lazy val sync = new SyncImpl(this)
        override lazy val async = new AsyncImpl(this)
    }

    private lazy val systemCollections = new SystemCollections()

    private[mongo] def apply(implicit context: Context): Database = {
        new DatabaseImpl(context)
    }
}
