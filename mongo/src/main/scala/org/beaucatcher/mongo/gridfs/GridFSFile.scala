package org.beaucatcher.mongo.gridfs

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

import java.util.Date

case class CreateOptions(filename : Option[String] = None, contentType : Option[String] = None,
    chunkSize : Option[Long] = None, aliases : Seq[String] = Nil, metadata : BObject = BObject.empty) {

    private[gridfs] def asBObjectWithNewId = {
        val b = BObject.newBuilder

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
class GridFSFile private[gridfs] (private[gridfs] val underlying : BObject) {
    /** get the file's _id */
    def _id = underlying.getUnwrappedAs[ObjectId]("_id")
    /** get the file's filename or throw NoSuchElementException */
    def filename = underlying.getUnwrappedAs[String]("filename")
    /** get the file's contentType or throw NoSuchElementException */
    def contentType = underlying.getUnwrappedAs[String]("contentType")
    /** get the file's length in bytes or throw NoSuchElementException */
    def length = underlying.getUnwrappedAs[Long]("length")
    /** get the file's chunk size or throw NoSuchElementException */
    def chunkSize = underlying.getUnwrappedAs[Long]("chunkSize") // using "Long" is silly (Input/OutputStream don't) but Java driver stores as Int64
    /** get the file's uploadDate or throw NoSuchElementException */
    def uploadDate = underlying.getUnwrappedAs[Date]("uploadDate")
    /** get the file's aliases or throw NoSuchElementException */
    def aliases : Seq[String] = underlying.getUnwrappedAs[List[String]]("aliases")
    /** get the file's metadata object or an empty object (throw NoSuchElementException only if there's a broken non-object under "metadata") */
    def metadata : BObject = underlying.get("metadata") match {
        case Some(obj : BObject) =>
            obj
        case Some(x) =>
            throw new NoSuchElementException("")
        case None =>
            BObject.empty
    }
    /** get the file's md5 or throw NoSuchElementException */
    def md5 = underlying.getUnwrappedAs[String]("md5")
}

object GridFSFile {
    /**
     * This creates a "blank" GridFSFile, but doesn't store it in mongodb
     * yet. The file is added to the collection only when you close the stream
     * from openForWriting(). So to create an empty file,
     * fs.openForWriting(GridFSFile()).close()
     */
    def apply(options : CreateOptions = CreateOptions.empty) = {
        new GridFSFile(options.asBObjectWithNewId)
    }
}
