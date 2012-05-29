package org.beaucatcher.mongo.gridfs

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.driver._
import org.beaucatcher.bson.Implicits._
import java.io._

// access to implementation collections, can be shared among GridFS objects for same bucket
private[gridfs] class GridFSCollections(val bucket: String) {
    private lazy val fileCodecs = new IteratorBasedCodecs[GridFSFile]() {
        override def toIterator(o: GridFSFile): Iterator[(String, Any)] = {
            MapCodecs.mapToIterator(o.underlying)
        }

        override def fromIterator(i: Iterator[(String, Any)]): GridFSFile = {
            new GridFSFile(MapCodecs.iteratorToMap(i))
        }
    }

    private lazy val fileCodecSet = {
        import MapCodecs.mapQueryResultDecoder
        implicit val valueDecoder = ValueDecoders.anyValueDecoder[Map[String, Any]]

        import fileCodecs._
        import IteratorCodecs._

        CollectionCodecSet[Iterator[(String, Any)], GridFSFile, GridFSFile, ObjectId, Any]()
    }

    private def createCollectionAccessWithEntity[EntityType <: AnyRef: Manifest, IdType: IdEncoder](name: String, entityCodecs: CollectionCodecSet[Iterator[(String, Any)], EntityType, EntityType, IdType, Any],
        migrateCallback: (CollectionAccessWithTwoEntityTypes[Iterator[(String, Any)], IdType, Map[String, Any], Any, EntityType, Any], Context) => Unit) = {
        CollectionAccessWithTwoEntityTypes[Iterator[(String, Any)], IdType, Map[String, Any], Any, EntityType, Any](name, migrateCallback)(CollectionCodecSetMap[IdType](), entityCodecs)
    }

    private def createCollectionAccessWithoutEntity[IdType: IdEncoder](name: String,
        migrateCallback: (CollectionAccessWithOneEntityType[Iterator[(String, Any)], Map[String, Any], IdType, Any], Context) => Unit) = {
        implicit val codecs = CollectionCodecSetMap[IdType]()
        CollectionAccessWithOneEntityType[Iterator[(String, Any)], Map[String, Any], IdType, Any](name, migrateCallback)
    }

    lazy val files: CollectionAccessWithTwoEntityTypes[Iterator[(String, Any)], ObjectId, Map[String, Any], Any, GridFSFile, Any] = {
        import IdEncoders.objectIdIdEncoder
        val access = createCollectionAccessWithEntity[GridFSFile, ObjectId](bucket + ".files",
            fileCodecSet,
            { (access, context) =>
                implicit val ctx = context
                // this isn't in the gridfs spec but it is in the Java implementation
                access.sync.ensureIndex(Iterator("filename" -> 1, "uploadDate" -> 1))
            })

        access
    }

    lazy val chunks: CollectionAccessWithOneEntityType[Iterator[(String, Any)], Map[String, Any], ObjectId, Any] = {
        import IdEncoders.objectIdIdEncoder
        val access = createCollectionAccessWithoutEntity[ObjectId](bucket + ".chunks",
            { (access, context) =>
                implicit val ctx = context
                access.sync.ensureIndex(Iterator("files_id" -> 1, "n" -> 1), IndexOptions(flags = Set(IndexUnique)))
            })

        access
    }

}

object GridFSCollections {

}

trait GridFS {
    def bucket: String

    protected def files: CollectionAccessWithTwoEntityTypes[Iterator[(String, Any)], ObjectId, Map[String, Any], Any, GridFSFile, Any]
    protected def chunks: CollectionAccessWithOneEntityType[Iterator[(String, Any)], Map[String, Any], ObjectId, Any]
}

object GridFS {
    private[gridfs] val DEFAULT_BUCKET = "fs"
    private[gridfs] val DEFAULT_CHUNK_SIZE = 256 * 1024L
}

// TODO use typeclasses for the query type
sealed trait SyncGridFS extends GridFS {
    def context: Context

    private[this] implicit final def implicitContext = context

    private[gridfs] def filesCollection = files.sync[GridFSFile]
    private[gridfs] def chunksCollection = chunks.sync[Map[String, Any]]

    /**
     * Obtain a read-only data access object for the bucket.files collection.
     * Use this to query for files.
     */
    def collection: BoundReadOnlySyncCollection[Iterator[(String, Any)], GridFSFile, ObjectId, _] = filesCollection

    /**
     * Delete one file by ID.
     */
    def removeById(id: ObjectId): WriteResult = {
        val result = filesCollection.removeById(id)
        if (result.ok)
            chunksCollection.remove(Iterator("files_id" -> id))
        else
            result
    }

    /**
     * Deletes all files matching the query. The query is against the files collection not the chunks collection,
     * but both the file and its chunks are removed.
     */
    def remove(query: Iterator[(String, Any)]): WriteResult = {
        // this is intended to avoid needing the entire query result in memory;
        // we should iterate the cursor lazily in chunks, theoretically.
        val ids = files.sync.find(query, IncludedFields.idOnly)
        val results = ids map { idObj => removeById(idObj.getOrElse("_id", throw new MongoException("no _id on files object")).asInstanceOf[ObjectId]) }
        // pick first failed result if any, otherwise an "ok"
        results.foldLeft(WriteResult(ok = true))({ (next, sofar) =>
            if (!next.ok && sofar.ok)
                next
            else
                sofar
        })
    }

    def remove(file: GridFSFile): WriteResult = {
        removeById(file._id)
    }

    def removeAll(): WriteResult = {
        files.sync.removeAll()
        chunks.sync.removeAll()
    }

    /**
     * Opens a gridfs file for reading its data.
     */
    def openForReading(file: GridFSFile): InputStream = {
        new GridFSInputStream(this, file)
    }

    /**
     * Opens a gridfs file for writing its data; the file has to be new and empty,
     * create the file object with a call to GridFSFile().
     */
    def openForWriting(file: GridFSFile): OutputStream = {
        new GridFSOutputStream(this, file)
    }
}

object SyncGridFS {
    private[gridfs] def newGridFSCollections(bucket: String): GridFSCollections = {
        import IdEncoders._
        val codecs = CollectionCodecSetBObject[ObjectId]()
        import codecs.{ collectionModifierEncoderEntity => _, collectionIdEncoder => _, _ }
        new GridFSCollections(bucket)
    }

    /**
     * Manually creates a SyncGridFS. You could also create an object that
     * extends GridFSAccess and use the "sync" field in that object.
     */
    def apply(bucket: String)(implicit context: Context): SyncGridFS = {
        new ConcreteSyncGridFS(newGridFSCollections(bucket), context)
    }
}

// this concrete class backends to the private GridFSCollections
private class ConcreteSyncGridFS(collections: GridFSCollections, override val context: Context) extends SyncGridFS {
    override def bucket = collections.bucket
    override protected def files = collections.files
    override protected def chunks = collections.chunks
}

trait GridFSAccessTrait {
    def bucket: String = GridFS.DEFAULT_BUCKET
    def sync(implicit context: Context): SyncGridFS
}

/**
 * This is intended to be subclassed by a global object that you then use
 * to access a given gridfs. "object MyFS extends GridFSAccess"
 * Override "def bucket" to specify a non-default bucket.
 */
abstract class GridFSAccess extends GridFSAccessTrait {
    // this is shared between the sync and async access objects
    private lazy val collections = SyncGridFS.newGridFSCollections(bucket)

    override def sync(implicit context: Context): SyncGridFS = new ConcreteSyncGridFS(collections, context)
}

class GridFSMongoException(message: String, cause: Throwable) extends MongoException(message, cause) {
    def this(message: String) = this(message, null)
}
