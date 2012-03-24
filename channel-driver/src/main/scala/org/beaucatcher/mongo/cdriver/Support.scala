package org.beaucatcher.mongo.cdriver

import org.beaucatcher.channel.netty._
import org.beaucatcher.bson._
import org.beaucatcher.wire.bson._
import org.beaucatcher.channel._
import org.beaucatcher.mongo._
import java.nio.ByteOrder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._

private[beaucatcher] object Support {

    implicit def bobjectQueryEncodeSupport: QueryEncodeSupport[BObject] =
        BObjectEncodeSupport

    implicit def bobjectEntityEncodeSupport: EntityEncodeSupport[BObject] =
        BObjectEncodeSupport

    implicit def bobjectEntityDecodeSupport: EntityDecodeSupport[BObject] =
        BObjectDecodeSupport

    private object BObjectEncodeSupport
        extends NettyEncodeSupport[BObject]
        with QueryEncodeSupport[BObject]
        with EntityEncodeSupport[BObject] {
        override final def write(buf: ChannelBuffer, t: BObject): Unit = {
            val start = buf.writerIndex()
            buf.ensureWritableBytes(32) // min size is 5, but prealloc for efficiency
            buf.writeInt(0) // will write this later
            for (field <- t.value) {
                writeValue(buf, field._1, field._2)
            }
            buf.ensureWritableBytes(1)
            buf.writeByte('\0')
            buf.setInt(start, buf.writerIndex - start)
        }
    }

    private[this] val intStrings = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20").toArray(manifest[String])

    private[this] def arrayIndex(i: Int): String = {
        if (i < intStrings.length)
            intStrings(i)
        else
            Integer.toString(i)
    }

    def writeArray(buf: ChannelBuffer, array: BArray): Unit = {
        val start = buf.writerIndex()
        buf.ensureWritableBytes(32) // some prealloc for efficiency
        buf.writeInt(0) // will write this later
        var i = 0
        for (element <- array.value) {
            writeValue(buf, arrayIndex(i), element)
            i += 1
        }
        buf.ensureWritableBytes(1)
        buf.writeByte('\0')
        buf.setInt(start, buf.writerIndex - start)
    }

    def writeValue(buf: ChannelBuffer, name: String, bvalue: BValue): Unit = {
        buf.ensureWritableBytes(1 + name.length() + 1 + 16) // typecode + name + nul + large value size
        bvalue match {
            case v: BInt32 =>
                buf.writeByte(NUMBER_INT)
                writeNulString(buf, name)
                buf.writeInt(v.value)
            case v: BInt64 =>
                buf.writeByte(NUMBER_LONG)
                writeNulString(buf, name)
                buf.writeLong(v.value)
            case v: BDouble =>
                buf.writeByte(NUMBER)
                writeNulString(buf, name)
                buf.writeDouble(v.value)
            case v: BObjectId =>
                buf.writeByte(OID)
                writeNulString(buf, name)
                buf.writeInt(swapInt(v.value.time))
                buf.writeInt(swapInt(v.value.machine))
                buf.writeInt(swapInt(v.value.inc))
            case v: BString =>
                buf.writeByte(STRING)
                writeNulString(buf, name)
                writeLengthString(buf, v.value)
            case v: BObject =>
                buf.writeByte(OBJECT)
                writeNulString(buf, name)
                writeQuery[BObject](buf, v, Int.MaxValue /* max size; already checked for outer object */ )
            case v: BArray =>
                buf.writeByte(ARRAY)
                writeNulString(buf, name)
                writeArray(buf, v)
            case v: BTimestamp =>
                buf.writeByte(TIMESTAMP)
                writeNulString(buf, name)
                buf.writeInt(v.value.inc)
                buf.writeInt(v.value.time)
            case v: BISODate =>
                buf.writeByte(DATE)
                writeNulString(buf, name)
                buf.writeLong(v.value.getMillis())
            case v: BBinary =>
                buf.writeByte(BINARY)
                writeNulString(buf, name)
                val bytes = v.value.data
                buf.ensureWritableBytes(bytes.length + 5)
                buf.writeInt(bytes.length)
                buf.writeByte(BsonSubtype.toByte(v.value.subtype))
                buf.writeBytes(bytes)
            case v: BBoolean =>
                buf.writeByte(BOOLEAN)
                writeNulString(buf, name)
                buf.writeByte(if (v.value) 1 else 0)
            case BNull =>
                buf.writeByte(NULL)
                writeNulString(buf, name)
            // no data on the wire for null
            case v: JObject =>
                throw new MongoException("Can't use JObject in queries: " + v)
            case v: JArray =>
                throw new MongoException("Can't use JArray in queries: " + v)
        }
    }

    private object BObjectDecodeSupport
        extends NettyDecodeSupport[BObject]
        with EntityDecodeSupport[BObject] {
        override final def read(buf: ChannelBuffer): BObject = {
            val len = buf.readInt()
            if (len == EMPTY_DOCUMENT_LENGTH) {
                BObject.empty
            } else {
                val b = BObject.newBuilder

                var what = buf.readByte()
                while (what != EOO) {

                    val name = readNulString(buf)

                    b += (name -> readBValue(what, buf))

                    what = buf.readByte()
                }

                b.result()
            }
        }
    }

    private def readArray(buf: ChannelBuffer): BArray = {
        val len = buf.readInt()
        if (len == EMPTY_DOCUMENT_LENGTH) {
            BArray.empty
        } else {
            val b = BArray.newBuilder

            var what = buf.readByte()
            while (what != EOO) {

                // the names in an array are just the indices, so nobody cares
                skipNulString(buf)

                b += readBValue(what, buf)

                what = buf.readByte()
            }

            b.result()
        }
    }

    private def readBValue(what: Byte, buf: ChannelBuffer): BValue = {
        what match {
            case NUMBER =>
                BDouble(buf.readDouble())
            case STRING =>
                BString(readLengthString(buf))
            case OBJECT =>
                readEntity[BObject](buf)
            case ARRAY =>
                readArray(buf)
            case BINARY =>
                val len = buf.readInt()
                val subtype = BsonSubtype.fromByte(buf.readByte()).getOrElse(BsonSubtype.GENERAL)
                val bytes = new Array[Byte](len)
                buf.readBytes(bytes)
                BBinary(Binary(bytes, subtype))
            case OID =>
                // the mongo wiki says the numbers are really
                // 4 bytes, 5 bytes, and 3 bytes but the java
                // driver does 4,4,4 and it looks like the C driver too.
                // so not sure what to make of it.
                // http://www.mongodb.org/display/DOCS/Object+IDs
                // the object ID is also big-endian unlike everything else
                val time = swapInt(buf.readInt())
                val machine = swapInt(buf.readInt())
                val inc = swapInt(buf.readInt())
                BObjectId(ObjectId(time, machine, inc))
            case BOOLEAN =>
                BBoolean(buf.readByte() != 0)
            case DATE =>
                BISODate(buf.readLong())
            case NULL =>
                BNull
            case NUMBER_INT =>
                BInt32(buf.readInt())
            case TIMESTAMP =>
                val inc = buf.readInt()
                val time = buf.readInt()
                BTimestamp(Timestamp(time, inc))
            case NUMBER_LONG =>
                BInt64(buf.readLong())
            case UNDEFINED |
                REGEX |
                REF |
                CODE |
                SYMBOL |
                CODE_W_SCOPE |
                MINKEY |
                MAXKEY =>
                throw new MongoException("Encountered value of type " + Integer.toHexString(what) + " which is currently unsupported")
        }
    }

}
