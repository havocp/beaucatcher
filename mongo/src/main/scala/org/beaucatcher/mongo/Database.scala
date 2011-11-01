package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

/**
 * Trait contains [[org.beaucatcher.mongo.CollectionOperations]] for some of mongo's
 * built-in collections. Obtain it through a [[org.beaucatcher.mongo.Database]] object,
 * you can't construct a [[org.beaucatcher.mongo.SystemCollections]] directly.
 */
final class SystemCollections private[mongo] (backend : MongoBackend) {
    private def createCollectionOperations[EntityType <: Product : Manifest, IdType : Manifest](name : String) = {
        val b = backend
        new CollectionOperationsWithCaseClass[EntityType, IdType] with MongoBackendProvider {
            override val backend = b
            override val collectionName = name
        }
    }

    lazy val indexes : CollectionOperationsTrait[CollectionIndex, String] = {
        createCollectionOperations[CollectionIndex, String]("system.indexes")
    }

    lazy val namespaces : CollectionOperationsTrait[Namespace, String] = {
        createCollectionOperations[Namespace, String]("system.namespaces")
    }

    // TODO system.users
}

case class CreateCollectionOptions(autoIndexId : Option[Boolean] = None, capped : Option[Boolean] = None,
    max : Option[Int] = None, size : Option[Int] = None, overrideQueryFlags : Option[Set[QueryFlag]] = None)

private[beaucatcher] object CreateCollectionOptions {
    final val empty = CreateCollectionOptions(None, None, None, None, None)
}

case class CommandOptions(overrideQueryFlags : Option[Set[QueryFlag]] = None)

private[beaucatcher] object CommandOptions {
    final val empty = CommandOptions(None)
}

trait SyncDatabase {
    protected def database : Database

    // TODO authenticate()
    // TODO addUser(), removeUser()
    // TODO eval()

    final def command(cmd : BObject) : CommandResult = command(cmd, CommandOptions.empty)

    def command(cmd : BObject, options : CommandOptions) : CommandResult

    final def createCollection(name : String) : CommandResult = {
        createCollection(name, CreateCollectionOptions.empty)
    }

    final def createCollection(name : String, options : CreateCollectionOptions) : CommandResult = {
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
                command(builder.result, CommandOptions(overrideQueryFlags))
            case _ =>
                throw new IllegalStateException("should not be reached")
        }
    }

    def collectionNames : Iterator[String] = {
        database.system.namespaces.sync[BObject].find(BObject.empty, IncludedFields("name")) map {
            obj =>
                obj.getUnwrappedAs[String]("name")
        }
    }

    def dropDatabase() : CommandResult = {
        command(BObject("dropDatabase" -> 1))
    }
}

trait Database {
    protected def backend : MongoBackend

    def name : String

    lazy val system : SystemCollections = new SystemCollections(backend)

    def sync : SyncDatabase
}
