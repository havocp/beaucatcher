package org.beaucatcher.mongo.gridfs

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.driver._
import org.beaucatcher.bson.Implicits._
import java.io._

// access to implementation collections, can be shared among GridFS objects for same bucket
private[gridfs] class GridFSCollections(driver : Driver, val bucket : String)(implicit bobjectQueryEncoder : QueryEncoder[BObject],
    bobjectQueryResultDecoder : QueryResultDecoder[BObject],
    bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject],
    bobjectUpsertEncoder : UpsertEncoder[BObject],
    bobjectModifierEncoder : ModifierEncoder[BObject],
    objectIdIdEncoder : IdEncoder[ObjectId],
    anyValueDecoder : ValueDecoder[Any]) {
    private lazy val fileCodecSet = {
        val fileCodecs = driver.newBObjectBasedCodecs[GridFSFile](f => f.underlying, obj => new GridFSFile(obj))
        import fileCodecs._

        CollectionCodecSet[BObject, GridFSFile, ObjectId, Any]()
    }

    private def createCollectionAccessWithEntity[EntityType <: AnyRef : Manifest, IdType : IdEncoder](name : String, entityCodecs : CollectionCodecSet[BObject, EntityType, IdType, Any],
        migrateCallback : (CollectionAccess[EntityType, IdType], Context) => Unit) = {
        val d = driver
        new CollectionAccess[EntityType, IdType] with DriverProvider {
            override val mongoDriver = d
            override val collectionName = name
            override val entityCodecSet = entityCodecs
            override def migrate(implicit context : Context) = migrateCallback(this, context)
        }
    }

    private def createCollectionAccessWithoutEntity[IdType : IdEncoder](name : String,
        migrateCallback : (CollectionAccessWithoutEntity[IdType], Context) => Unit) = {
        val d = driver
        new CollectionAccessWithoutEntity[IdType] with DriverProvider {
            override val mongoDriver = d
            override val collectionName = name

            override def migrate(implicit context : Context) = migrateCallback(this, context)
        }
    }

    lazy val files : CollectionAccessTrait[GridFSFile, ObjectId] = {
        import Codecs._
        val access = createCollectionAccessWithEntity[GridFSFile, ObjectId](bucket + ".files",
            fileCodecSet,
            { (access, context) =>
                // this isn't in the gridfs spec but it is in the Java implementation
                access.sync(context).ensureIndex(BObject("filename" -> 1, "uploadDate" -> 1))
            })

        access
    }

    lazy val chunks : CollectionAccessWithoutEntityTrait[ObjectId] = {
        import Codecs._
        val access = createCollectionAccessWithoutEntity[ObjectId](bucket + ".chunks",
            { (access, context) =>
                access.sync(context).ensureIndex(BObject("files_id" -> 1, "n" -> 1), IndexOptions(flags = Set(IndexUnique)))
            })

        access
    }

}

object GridFSCollections {

}

trait GridFS {
    def bucket : String

    protected def files : CollectionAccessTrait[GridFSFile, ObjectId]
    protected def chunks : CollectionAccessWithoutEntityTrait[ObjectId]
}

object GridFS {
    private[gridfs] val DEFAULT_BUCKET = "fs"
    private[gridfs] val DEFAULT_CHUNK_SIZE = 256 * 1024L
}

sealed trait SyncGridFS extends GridFS {
    def context : Context

    private[this] implicit final def implicitContext = context

    private[gridfs] def filesCollection = files.sync[GridFSFile]
    private[gridfs] def chunksCollection = chunks.sync

    /**
     * Obtain a read-only data access object for the bucket.files collection.
     * Use this to query for files.
     */
    def collection : ReadOnlySyncCollection[BObject, GridFSFile, ObjectId, _] = filesCollection

    /**
     * Delete one file by ID.
     */
    def removeById(id : ObjectId) : WriteResult = {
        val result = filesCollection.removeById(id)
        if (result.ok)
            chunksCollection.remove(BObject("files_id" -> id))
        else
            result
    }

    /**
     * Deletes all files matching the query. The query is against the files collection not the chunks collection,
     * but both the file and its chunks are removed.
     */
    def remove(query : BObject) : WriteResult = {
        // this is intended to avoid needing the entire query result in memory;
        // we should iterate the cursor lazily in chunks, theoretically.
        val ids = files.sync[BObject].find(query, IncludedFields.idOnly)
        val results = ids map { idObj => removeById(idObj.getUnwrappedAs[ObjectId]("_id")) }
        // pick first failed result if any, otherwise an "ok"
        results.foldLeft(WriteResult(ok = true))({ (next, sofar) =>
            if (!next.ok && sofar.ok)
                next
            else
                sofar
        })
    }

    def remove(file : GridFSFile) : WriteResult = {
        removeById(file._id)
    }

    def removeAll() : WriteResult = {
        files.sync.removeAll()
        chunks.sync.removeAll()
    }

    /**
     * Opens a gridfs file for reading its data.
     */
    def openForReading(file : GridFSFile) : InputStream = {
        new GridFSInputStream(this, file)
    }

    /**
     * Opens a gridfs file for writing its data; the file has to be new and empty,
     * create the file object with a call to GridFSFile().
     */
    def openForWriting(file : GridFSFile) : OutputStream = {
        new GridFSOutputStream(this, file)
    }
}

object SyncGridFS {
    private[gridfs] def newGridFSCollections(driver : Driver, bucket : String) : GridFSCollections = {
        implicit val idEncoder = driver.newObjectIdIdEncoder
        val codecs = driver.newBObjectCodecSet[ObjectId]()
        import codecs.{ collectionModifierEncoderEntity => _, collectionIdEncoder => _, _ }
        new GridFSCollections(driver, bucket)
    }

    /**
     * Manually creates a SyncGridFS. You could also create an object that
     * extends GridFSAccess and use the "sync" field in that object.
     */
    def apply(bucket : String)(implicit context : Context) : SyncGridFS = {
        new ConcreteSyncGridFS(newGridFSCollections(context.driver, bucket), context)
    }
}

// this concrete class backends to the private GridFSCollections
private class ConcreteSyncGridFS(collections : GridFSCollections, override val context : Context) extends SyncGridFS {
    override def bucket = collections.bucket
    override protected def files = collections.files
    override protected def chunks = collections.chunks
}

trait GridFSAccessTrait {
    def bucket : String = GridFS.DEFAULT_BUCKET
    def sync(implicit context : Context) : SyncGridFS
}

/**
 * This is intended to be subclassed by a global object that you then use
 * to access a given gridfs. "object MyFS extends GridFSAccess"
 * Override "def bucket" to specify a non-default bucket.
 */
abstract class GridFSAccess extends GridFSAccessTrait {
    self : DriverProvider =>

    // this is shared between the sync and async access objects
    private lazy val collections = SyncGridFS.newGridFSCollections(mongoDriver, bucket)

    override def sync(implicit context : Context) : SyncGridFS = new ConcreteSyncGridFS(collections, context)
}

class GridFSMongoException(message : String, cause : Throwable) extends MongoException(message, cause) {
    def this(message : String) = this(message, null)
}
