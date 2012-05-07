package org.beaucatcher.mongo.gridfs

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

import java.util.Date

case class CreateOptions(filename: Option[String] = None, contentType: Option[String] = None,
    chunkSize: Option[Long] = None, aliases: Seq[String] = Nil, metadata: Map[String, Any] = Map.empty) {

    private[gridfs] def asMapWithNewId = {
        val b = Map.newBuilder[String, Any]

        b += ("_id" -> ObjectId())

        filename.foreach(fn => b += ("filename" -> fn))
        contentType.foreach(ct => b += ("contentType" -> ct))
        val chunkSizeValue = chunkSize.getOrElse(GridFS.DEFAULT_CHUNK_SIZE)
        b += ("chunkSize" -> chunkSizeValue)
        if (aliases.nonEmpty)
            b += ("aliases" -> aliases)
        if (metadata.nonEmpty)
            b += ("metadata" -> metadata)

        // we want these fields to always exist... if you never write to the file,
        // it still exists, it just has no contents
        b += ("length" -> 0)
        b += ("md5" -> "d41d8cd98f00b204e9800998ecf8427e") // md5 of zero bytes

        b.result
    }
}

object CreateOptions {
    val empty = CreateOptions()
}

/**
 * Represents a file "handle" to the gridfs; does not represent the
 * file data, the data is manipulated as a stream.
 */
class GridFSFile private[gridfs] (private[gridfs] val underlying: Map[String, Any]) {
    private def getAs[A: Manifest](key: String): A = {
        // FIXME the BValue.wrap.unwrappedAs hack is a way to use the
        // cast method from BValue, which does numeric conversions.
        // Export it properly.
        underlying.get(key).map(BValue.wrap(_).unwrappedAs[A]).getOrElse(throw new BugInSomethingMongoException("Missing field in GridFS file: " + key))
    }

    /** get the file's _id */
    def _id = getAs[ObjectId]("_id")
    /** get the file's filename or throw NoSuchElementException */
    def filename = getAs[String]("filename")
    /** get the file's contentType or throw NoSuchElementException */
    def contentType = getAs[String]("contentType")
    /** get the file's length in bytes or throw NoSuchElementException */
    def length = getAs[Long]("length")
    /** get the file's chunk size or throw NoSuchElementException */
    def chunkSize = getAs[Long]("chunkSize") // using "Long" is silly (Input/OutputStream don't) but Java driver stores as Int64
    /** get the file's uploadDate or throw NoSuchElementException */
    def uploadDate = getAs[Date]("uploadDate")
    /** get the file's aliases or throw NoSuchElementException */
    def aliases: Seq[String] = getAs[List[String]]("aliases")
    /** get the file's metadata object or an empty object (throw NoSuchElementException only if there's a broken non-object under "metadata") */
    def metadata: Map[String, Any] = underlying.get("metadata") match {
        case Some(obj: Map[_, _]) =>
            obj.asInstanceOf[Map[String, Any]]
        case Some(x) =>
            throw new NoSuchElementException("")
        case None =>
            Map.empty
    }
    /** get the file's md5 or throw NoSuchElementException */
    def md5 = getAs[String]("md5")

    override def toString = "GridFSFile(%s)".format(underlying)
}

object GridFSFile {
    /**
     * This creates a "blank" GridFSFile, but doesn't store it in mongodb
     * yet. The file is added to the collection only when you close the stream
     * from openForWriting(). So to create an empty file,
     * fs.openForWriting(GridFSFile()).close()
     */
    def apply(options: CreateOptions = CreateOptions.empty) = {
        new GridFSFile(options.asMapWithNewId)
    }
}
