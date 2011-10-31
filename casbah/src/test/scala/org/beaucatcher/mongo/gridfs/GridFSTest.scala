package org.beaucatcher.mongo.gridfs

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.gridfs._
import org.beaucatcher.casbah.CasbahTestProvider
import org.junit.Assert._
import org.junit._

import org.apache.commons.io._

object TestFS extends GridFSOperations with CasbahTestProvider {
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

    @Test
    def createAndReadSmallChunkFiles() {
        // create all the files
        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            // a chunkSize of 4 is ludicrous, it's just to allow us to test
            // multi-chunk handling.
            val f = GridFSFile(CreateOptions(filename = Some(name), chunkSize = Some(4)))
            val stream = TestFS.sync.openForWriting(f)
            for (i <- 1 to 1000) {
                stream.write(c)
            }
            stream.close()
            assertEquals(f._id, TestFS.sync.dao.findOneById(f._id).get._id)
        }
        for (c <- 'a' to 'f') {
            val name = "contains-" + c
            val f = TestFS.sync.dao.findOne(BObject("filename" -> name)).get
            assertEquals(4, f.chunkSize)
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
}
