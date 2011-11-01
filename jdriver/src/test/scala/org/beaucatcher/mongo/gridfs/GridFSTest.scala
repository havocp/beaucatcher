package org.beaucatcher.mongo.gridfs

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.gridfs._
import org.beaucatcher.jdriver.JavaDriverTestProvider
import org.junit.Assert._
import org.junit._
import org.apache.commons.io._
import java.io.OutputStream

object TestFS extends GridFSOperations with JavaDriverTestProvider {
    override def bucket = "testbucket"
}

class GridFSTest extends TestUtils {
    @org.junit.Before
    def setup() {
        TestFS.sync.removeAll()
        assertEquals(0, TestFS.sync.dao.count())
    }

    @org.junit.After
    def teardown() {
        TestFS.sync.removeAll()
    }

    @Test
    def createAndReadEmptyFile() {
        val f = GridFSFile()
        TestFS.sync.openForWriting(f).close()
        assertEquals(0, f.length)
        assertTrue(TestFS.sync.dao.findOneById(f._id).isDefined)
        val stream = TestFS.sync.openForReading(f)
        val content = IOUtils.toString(stream)
        stream.close()
        assertEquals("", content)
        TestFS.sync.remove(f)
        assertEquals(None, TestFS.sync.dao.findOneById(f._id))
    }

    @Test
    def createAndReadOneByteFiles() {
        // create all the files
        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            val f = GridFSFile(CreateOptions(filename = Some(name)))
            val stream = TestFS.sync.openForWriting(f)
            stream.write(c)
            stream.close()
            assertEquals(f._id, TestFS.sync.dao.findOneById(f._id).get._id)
        }
        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            val f = TestFS.sync.dao.findOne(BObject("filename" -> name)).get
            val stream = TestFS.sync.openForReading(f)
            val content = IOUtils.toString(stream)
            stream.close()
            assertEquals(c + "", content)
            TestFS.sync.remove(f)
            assertEquals(None, TestFS.sync.dao.findOneById(f._id))
        }
    }

    // This is a prime to better test buffer-copying code
    private val SMALL_CHUNK_SIZE = 7L

    private def writeCharVaryingBufferLengths(stream : OutputStream, c : Char, count : Int) {
        var remaining = count
        var bufferLength = 5
        while (remaining > 0) {
            val toWrite = math.min(bufferLength, remaining)
            if (bufferLength == 1) {
                // test single-char write() also
                stream.write(c)
            } else {
                val buf = new Array[Byte](toWrite)
                for (i <- 0 until buf.length)
                    buf.update(i, c.toByte) // whee, assume ascii
                stream.write(buf)
            }
            remaining -= toWrite
            require(remaining >= 0)
            bufferLength -= 1
            if (bufferLength == 0)
                bufferLength = 5
            require(bufferLength > 0)
        }
    }

    private def createSmallChunkFiles() {
        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            // a chunkSize of 7 is ludicrous, it's just to allow us to test
            // multi-chunk handling.
            val f = GridFSFile(CreateOptions(filename = Some(name), chunkSize = Some(SMALL_CHUNK_SIZE)))
            val stream = TestFS.sync.openForWriting(f)
            writeCharVaryingBufferLengths(stream, c, 1000)
            stream.close()
            val saved = TestFS.sync.dao.findOneById(f._id).get
            assertEquals(f._id, saved._id)
            // this assumes we only wrote ASCII chars
            assertEquals(1000, saved.length)
        }
    }

    private def readAndRemoveSmallChunkFiles() {
        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            val f = TestFS.sync.dao.findOne(BObject("filename" -> name)).get
            assertEquals(SMALL_CHUNK_SIZE, f.chunkSize)
            val stream = TestFS.sync.openForReading(f)
            val content = IOUtils.toString(stream)
            stream.close()
            assertEquals(1000, content.length)
            assertEquals(c, content(0))
            assertEquals(c, content(999))
            TestFS.sync.remove(f)
            assertEquals(None, TestFS.sync.dao.findOneById(f._id))
        }
    }

    @Test
    def createAndReadSmallChunkFiles() {
        createSmallChunkFiles()
        readAndRemoveSmallChunkFiles()
    }

    @Test
    def storeMetadata() {
        val metadata = BObject("a" -> 1, "b" -> "hi")
        val f = GridFSFile(CreateOptions(metadata = metadata))
        assertEquals(metadata, f.metadata)
        TestFS.sync.openForWriting(f).close()

        val reloaded = TestFS.sync.dao.findOneById(f._id).get
        assertEquals(metadata, reloaded.metadata)

        TestFS.sync.remove(f)
    }

    private class JavaFS {
        import com.mongodb._
        import com.mongodb.util._
        import com.mongodb.gridfs._

        val uri = new MongoURI(TestFS.backend.config.url)
        val mongo = new Mongo(uri)
        val db = mongo.getDB(uri.getDatabase())
        val fs = new GridFS(db, TestFS.bucket)

        def close() {
            mongo.close()
        }
    }

    private def javaCreateSmallChunkFiles() {
        import com.mongodb._

        val jfs = new JavaFS()

        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            val f = jfs.fs.createFile()
            f.setFilename(name)
            f.setChunkSize(SMALL_CHUNK_SIZE)
            val stream = f.getOutputStream()
            writeCharVaryingBufferLengths(stream, c, 1000)
            stream.close()
            val saved = jfs.fs.findOne(f.getId().asInstanceOf[org.bson.types.ObjectId])
            assertEquals(f.getId(), saved.getId())
            // this assertion fails right now due to a Java driver bug
            assertEquals(1000, saved.getLength())
        }

        jfs.close()
    }

    private def javaReadAndRemoveSmallChunkFiles() {
        import com.mongodb._

        val jfs = new JavaFS()

        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            val f = jfs.fs.findOne(new BasicDBObject("filename", name))
            assertEquals(SMALL_CHUNK_SIZE, f.getChunkSize())
            val stream = f.getInputStream()
            val content = IOUtils.toString(stream)
            stream.close()
            assertEquals(1000, content.length)
            assertEquals(c, content(0))
            assertEquals(c, content(999))
            jfs.fs.remove(f.getId().asInstanceOf[org.bson.types.ObjectId])
            val removed = jfs.fs.findOne(new BasicDBObject("filename", name))
            assertNull(removed)
        }

        jfs.close()
    }

    // @Test // this test fails for now because Java driver writes out wrong file length
    def javaDriverCreateAndReadSmallChunkFiles() {
        // be sure java driver interoperates with itself before we try to interoperate with it
        javaCreateSmallChunkFiles()
        javaReadAndRemoveSmallChunkFiles()
    }

    @Test
    def javaDriverReadsOurFile() {
        createSmallChunkFiles()
        javaReadAndRemoveSmallChunkFiles()
    }

    // @Test // this test fails for now because Java driver writes out wrong file length
    def javaDriverWritesSomethingWeRead() {
        javaCreateSmallChunkFiles()
        readAndRemoveSmallChunkFiles()
    }

    private def createOneByteFile(filename : String, contentType : String, aliases : Seq[String]) = {
        val f = GridFSFile(CreateOptions(filename = Some(filename), contentType = Some(contentType), aliases = aliases))
        val stream = TestFS.sync.openForWriting(f)
        stream.write('a')
        stream.close()
        f._id
    }

    private def javaCreateOneByteFile(filename : String, contentType : String, aliases : Seq[String]) = {
        import scala.collection.JavaConverters._

        val jfs = new JavaFS()

        val f = jfs.fs.createFile()
        f.setFilename(filename)
        f.setContentType(contentType)
        f.put("aliases", aliases.asJava)

        val stream = f.getOutputStream()
        stream.write('a')
        stream.close()

        jfs.close()
        ObjectId(f.getId().toString())
    }

    @Test
    def javaDriverCreatesMatchingObjects() {
        // There is one difference with the Java driver that we aren't
        // triggering here on purpose, which is that it stores "null"
        // for contentType, aliases, and filename if those are unset,
        // while we just omit them from the database in that case.
        // Seems like omitting should work, we'll see.

        val ourId = createOneByteFile("foo", "text/plain", Seq("bar"))
        val jId = javaCreateOneByteFile("foo", "text/plain", Seq("bar"))

        val ourFile = TestFS.sync.dao.findOneById(ourId).get
        val jFile = TestFS.sync.dao.findOneById(jId).get

        assertEquals(ourId, ourFile._id)
        assertEquals(jId, jFile._id)

        assertTrue(ourFile.underlying.contains("uploadDate"))
        assertTrue(jFile.underlying.contains("uploadDate"))

        assertEquals(Seq("bar"), ourFile.aliases)
        assertEquals(Seq("bar"), jFile.aliases)

        // strip _id and uploadDate because they are not supposed to match.
        val ourStripped = ourFile.underlying - "_id" - "uploadDate"
        val jStripped = jFile.underlying - "_id" - "uploadDate"

        assertEquals(ourStripped.toSeq.sortBy(_._1),
            jStripped.toSeq.sortBy(_._1))

        TestFS.sync.removeById(ourId)
        TestFS.sync.removeById(jId)
    }
}
