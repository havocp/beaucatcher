package org.beaucatcher.mongo.gridfs

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo._
import java.io.OutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.util.concurrent.ArrayBlockingQueue

private[gridfs] class GridFSOutputStream(fs : SyncGridFS, file : GridFSFile) extends OutputStream {
    // in theory, re-opening an existing file could do something useful, but not implemented
    // and perhaps dangerous for anyone to use, anyhow (how do you keep people from using
    // the file as it's being modified?)
    if (file.length > 0)
        throw new GridFSMongoException("File opened for writing but already has chunks: " + file)

    if (file.chunkSize > Int.MaxValue)
        throw new GridFSMongoException("File opened for writing but chunks are too big for OutputStream API: " + file)

    private val bufSize = file.chunkSize.toInt
    private val buf = new ByteArrayOutputStream(bufSize)
    private val digest = GridFSOutputStream.digestCache.acquire
    private var chunkNumber = 0
    private var totalCount = 0
    private var closed = false
    private var failed = false

    private def flushChunk() {
        if (buf.size > 0) {
            val bytes = buf.toByteArray()
            buf.reset()
            val chunk = BObject("_id" -> ObjectId(),
                "files_id" -> file._id,
                "n" -> chunkNumber,
                "data" -> Binary(bytes))

            chunkNumber += 1
            totalCount += bytes.length
            digest.update(bytes)

            try {
                fs.chunksDAO.save(chunk).throwIfNotOk
            } catch {
                case ex : MongoException =>
                    failed = true
                    close()
                    throw new IOException("Error writing gridfs chunk to mongo", ex)
            }
        }
    }

    private def precheck() {
        require(buf.size < bufSize) // bug in our code if this happens
        if (failed)
            throw new IOException("OutputStream to gridfs no longer usable due to an earlier exception")
        if (closed)
            throw new IOException("OutputStream to gridfs file has already been closed")
    }

    private def postcheck() {
        require(buf.size <= bufSize)
        if (buf.size == bufSize)
            flushChunk()
    }

    override def write(b : Int) {
        precheck()
        buf.write(b)
        postcheck()
    }

    override def write(bytes : Array[Byte], offset : Int, len : Int) {
        require(bytes != null)

        var start = offset
        var remaining = len

        while (remaining > 0) {
            precheck()

            val available = bufSize - buf.size
            require(available > 0)

            val toWrite = math.min(remaining, available)

            buf.write(bytes, start, toWrite)
            remaining -= toWrite
            start += toWrite

            require(remaining >= 0)
            require(start <= bytes.length)

            postcheck()
        }
    }

    override def close() {
        if (!closed) {
            // KEEP IN MIND this can be called on a db or IO error, in addition to the normal case
            try {
                closed = true
                if (!failed) {
                    flushChunk()

                    val md5 = toHex(digest.digest())

                    val newFileFields = BObject("md5" -> md5,
                        "length" -> totalCount)
                    val newFile = new GridFSFile(file.underlying ++ newFileFields)

                    try {
                        fs.filesDAO.save(newFile).throwIfNotOk
                    } catch {
                        case ex : MongoException =>
                            throw new IOException("Failed to write file md5 and length, file " + newFile, ex)
                    }
                }
            } finally {
                require(closed)
                GridFSOutputStream.digestCache.release(digest)
            }
        }
    }
}

object GridFSOutputStream {
    private abstract class Cache[T](maxToCache : Int = java.lang.Runtime.getRuntime().availableProcessors) {
        private val queue = new ArrayBlockingQueue[T](maxToCache)
        def create : T

        def acquire : T = {
            val t = queue.poll()
            if (t == null)
                create
            else
                t
        }

        def release(t : T) {
            queue.offer(t) // no-op if the capacity is exceeded
        }
    }

    private val digestCache = new Cache[MessageDigest] {
        override def create = MessageDigest.getInstance("MD5")
    }
}
