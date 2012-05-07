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
final class SystemCollections private[mongo] (driver: Driver) {
    import IdEncoders._

    private def createCollectionAccess[EntityType <: Product: Manifest, IdType: IdEncoder](name: String) = {
        val d = driver
        new CollectionAccessWithCaseClass[EntityType, IdType] with DriverProvider {
            override val mongoDriver = d
            override val collectionName = name
        }
    }

    private lazy val _indexes = {
        createCollectionAccess[CollectionIndex, String]("system.indexes")
    }

    def indexes: CollectionAccessTrait[CollectionIndex, String] = _indexes

    private lazy val _namespaces = {
        createCollectionAccess[Namespace, String]("system.namespaces")
    }

    def namespaces: CollectionAccessTrait[Namespace, String] = _namespaces

    // TODO system.users
}

case class CreateCollectionOptions(autoIndexId: Option[Boolean] = None, capped: Option[Boolean] = None,
    max: Option[Int] = None, size: Option[Int] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None)

private[beaucatcher] object CreateCollectionOptions {
    final val empty = CreateCollectionOptions(None, None, None, None, None)

    private[beaucatcher] def buildCommand(name: String, options: CreateCollectionOptions): BObject = {
        val builder = BObject.newBuilder
        // the Mongo docs say "createCollection" but driver uses "create"
        builder += ("create" -> name)
        // use "match" here so it will break if we modify CreateCollectionOptions fields
        options match {
            case CreateCollectionOptions(autoIndexId, capped, max, size, overrideQueryFlags) =>
                autoIndexId foreach { v => builder += ("autoIndexId" -> v) }
                capped foreach { v => builder += ("capped" -> v) }
                max foreach { v => builder += ("max" -> v) }
                size foreach { v => builder += ("size" -> v) }
                builder.result
            case _ =>
                throw new IllegalStateException("should not be reached")
        }
        builder.result
    }
}

trait SyncDatabase {
    protected[mongo] def database: Database

    private implicit def context: Context = database.context

    private lazy val underlying = context.driverContext.database.sync

    // TODO authenticate()
    // TODO addUser(), removeUser()
    // TODO eval()

    final def command(cmd: BObject): CommandResult = command(cmd, CommandOptions.empty)

    def command(cmd: BObject, options: CommandOptions): CommandResult = {
        import BObjectCodecs._
        underlying.command(cmd, options)
    }

    final def createCollection(name: String): CommandResult = {
        createCollection(name, CreateCollectionOptions.empty)
    }

    final def createCollection(name: String, options: CreateCollectionOptions): CommandResult = {
        command(CreateCollectionOptions.buildCommand(name, options), CommandOptions(options.overrideQueryFlags))
    }

    // TODO should return Cursor which requires Cursor to have CanBuildFrom machinery
    def collectionNames: Iterator[String] = {
        database.system.namespaces.sync[BObject].find(BObject.empty, IncludedFields("name")) map {
            obj =>
                obj.getUnwrappedAs[String]("name")
        }
    }

    def dropDatabase(): CommandResult = {
        command(BObject("dropDatabase" -> 1))
    }
}

trait AsyncDatabase {
    protected[mongo] def database: Database

    private implicit def context: Context = database.context

    private lazy val underlying = context.driverContext.database.async

    // TODO authenticate()
    // TODO addUser(), removeUser()
    // TODO eval()

    final def command(cmd: BObject): Future[CommandResult] = command(cmd, CommandOptions.empty)

    def command(cmd: BObject, options: CommandOptions): Future[CommandResult] = {
        import BObjectCodecs._
        underlying.command(cmd, options)
    }

    final def createCollection(name: String): Future[CommandResult] = {
        createCollection(name, CreateCollectionOptions.empty)
    }

    final def createCollection(name: String, options: CreateCollectionOptions): Future[CommandResult] = {
        command(CreateCollectionOptions.buildCommand(name, options), CommandOptions(options.overrideQueryFlags))
    }

    def collectionNames: Future[AsyncCursor[String]] = {
        database.system.namespaces.async[BObject].find(BObject.empty, IncludedFields("name")) map { cursor =>
            cursor.map({ obj =>
                obj.getUnwrappedAs[String]("name")
            })
        }
    }

    def dropDatabase(): Future[CommandResult] = {
        command(BObject("dropDatabase" -> 1))
    }
}

trait Database {
    def context: Context

    def name: String

    private lazy val _system = new SystemCollections(context.driver)

    def system: SystemCollections = _system

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

    def apply(implicit context: Context): Database = {
        new DatabaseImpl(context)
    }
}
