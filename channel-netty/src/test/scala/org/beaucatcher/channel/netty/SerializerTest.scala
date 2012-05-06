package org.beaucatcher.channel.netty

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.wire._
import org.junit.Assert._
import org.junit._
import java.nio.ByteOrder
import org.jboss.netty.buffer.ChannelBuffers
import java.util.Random

class SerializerTest extends TestUtils {

    private def testRoundTrip[O](obj: O)(implicit encoder: QueryEncoder[O], decoder: QueryResultDecoder[O]): Unit = {
        import CodecUtils._

        val buf = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 128)

        writeQuery(Buffer(buf), obj, Mongo.DEFAULT_MAX_DOCUMENT_SIZE)

        /*
        System.err.println("Wrote " + bobj + " to buffer: " + buf)

        buf.resetReaderIndex()
        var i = 0
        while (i < buf.writerIndex()) {
            if (i % 4 == 0)
                System.err.println("  " + buf.getInt(i))
            val b = buf.getByte(i)
            System.err.println("" + i + ": 0x" + Integer.toHexString(b))
            if (Character.isLetter(b))
                System.err.println("    " + Character.toString(b.toChar))
            i += 1
        }
        */

        buf.resetReaderIndex()
        val decoded = readEntity[O](Buffer(buf))

        assertEquals(obj, decoded)
    }

    @Test
    def roundTripBObject(): Unit = {
        import BObjectCodecs._
        val many = BsonTest.makeObjectManyTypes()
        // first test each field separately
        for (field <- many.value) {
            testRoundTrip(BObject(List(field)))
        }
        // then test the whole giant object
        testRoundTrip(many)
    }

    @Test
    def roundTripMap(): Unit = {
        import MapCodecs._
        val many: Map[String, Any] = BsonTest.makeObjectManyTypes().unwrapped
        // first test each field separately
        for (field <- many) {
            testRoundTrip(Map(field._1 -> field._2))
        }
        // then test the whole giant object
        testRoundTrip(many)
    }

    private def mapValuesToIterators(m: Map[String, Any]): Map[String, Any] = {
        m.mapValues({ v =>
            v match {
                case m: Map[_, _] =>
                    m.iterator
                case v =>
                    v
            }
        })
    }

    private def growBufferWhenNeeded[O](obj: O)(implicit encoder: QueryEncoder[O]): Unit = {
        import CodecUtils._

        val rand = new Random(1234L)
        val buf = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1)

        for (i <- 1 to 100) {
            // nextInt(x) is exclusive of x, so we write between 0 and 7
            var j = rand.nextInt(8)
            while (j > 0) {
                buf.ensureWritableBytes(1)
                buf.writeByte('X')
                j -= 1
            }
            writeQuery(Buffer(buf), obj, Mongo.DEFAULT_MAX_DOCUMENT_SIZE)
        }
    }

    @Test
    def growBufferWhenNeededBObject(): Unit = {
        import BObjectCodecs._

        val many = BsonTest.makeObjectManyTypes()
        for (field <- many.value) {
            growBufferWhenNeeded(BObject(List(field)))
        }
        growBufferWhenNeeded(many)
    }

    @Test
    def growBufferWhenNeededMap(): Unit = {
        import MapCodecs._

        val many: Map[String, Any] = BsonTest.makeObjectManyTypes().unwrapped
        for (field <- many) {
            growBufferWhenNeeded(Map(field._1 -> field._2))
        }
        growBufferWhenNeeded(many)
    }

    @Test
    def growBufferWhenNeededIterator(): Unit = {
        import IteratorCodecs._

        val many: Map[String, Any] = BsonTest.makeObjectManyTypes().unwrapped
        for (field <- mapValuesToIterators(many)) {
            growBufferWhenNeeded(Iterator(field._1 -> field._2))
        }
        growBufferWhenNeeded(mapValuesToIterators(many).iterator)
    }
}
