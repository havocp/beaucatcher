package org.beaucatcher.mongo.gridfs

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import scala.collection.JavaConverters._
import java.io.InputStream
import java.io.SequenceInputStream
import java.io.IOException
import java.io.ByteArrayInputStream

private[gridfs] class GridFSInputStream(fs : SyncGridFS, file : GridFSFile)
    extends SequenceInputStream(GridFSInputStream.chunkStreams(fs, file)) {
}

object GridFSInputStream {

    private def chunkStreams(fs : SyncGridFS, file : GridFSFile) : java.util.Enumeration[InputStream] = {
        // a quirk of this implementation is that it "succeeds" if GridFSFile is not even
        // in mongodb, if the file is zero-length. But throwing an error in that non-real-world case does
        // not seem worth an extra round trip to mongodb in every real world case.

        // the weird cast is because BDouble and Double are both in scope with intValue methods
        val numChunks = (math.ceil((file.length : Double) / file.chunkSize) : java.lang.Double).intValue

        // this all needs to be lazy, which I _think_ it should be...
        // the ".iterator" allows us to later use .asJavaEnumeration which isn't allowed on a sequence.
        val chunks = for (n <- (0 until numChunks).iterator)
            yield fs.chunksDAO.findOne(BObject("files_id" -> file._id, "n" -> n)).getOrElse(throw new IOException("Missing chunk " + n + "/" + numChunks + " of file " + file))
        val streams = chunks map { chunk =>
            chunk.get("data") match {
                case Some(BBinary(bin)) =>
                    new ByteArrayInputStream(bin.data) : InputStream
                case _ =>
                    throw new IOException("Chunk " + chunk + " of file " + file + " has bad data")
            }
        }
        // we put an empty stream first because SequenceInputStream gets the first enumeration item
        // on construct, but freaks out if there's an IOException on construct. So we want the first
        // stream to be harmless so we get the IOException only on read.
        val emptyStream = new ByteArrayInputStream(new Array[Byte](0))
        (Iterator(emptyStream) ++ streams).asJavaEnumeration
    }
}
