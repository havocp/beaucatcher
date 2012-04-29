package org.beaucatcher.mongo.cdriver

import org.beaucatcher.channel.netty._
import org.beaucatcher.bson._
import org.beaucatcher.wire._
import org.beaucatcher.channel._
import org.beaucatcher.mongo._
import java.nio.ByteOrder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._

private[beaucatcher] object Support {

    implicit def bobjectQueryEncoder: QueryEncoder[BObject] =
        BObjectEncodeSupport

    implicit def bobjectEntityEncodeSupport: EntityEncodeSupport[BObject] =
        BObjectEncodeSupport

    implicit def bobjectQueryResultDecoder: QueryResultDecoder[BObject] =
        BObjectDecodeSupport

    private[cdriver] def writeOpenDocument(buf: ChannelBuffer): Int = {
        val start = buf.writerIndex()
        buf.ensureWritableBytes(32) // min size is 5, but prealloc for efficiency
        buf.writeInt(0) // will write this later
        start
    }

    private[cdriver] def writeCloseDocument(buf: ChannelBuffer, start: Int) = {
        buf.ensureWritableBytes(1)
        buf.writeByte('\0')
        buf.setInt(start, buf.writerIndex - start)
    }

    private[cdriver] object BObjectEncodeSupport
        extends NettyDocumentEncoder[BObject]
        with QueryEncoder[BObject]
        with EntityEncodeSupport[BObject] {
        override final def write(buf: ChannelBuffer, t: BObject): Unit = {
            val start = writeOpenDocument(buf)

            for (field <- t.value) {
                writeValue(buf, field._1, field._2)
            }

            writeCloseDocument(buf, start)
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

    final private[cdriver] def writeFieldInt(buf: ChannelBuffer, name: String, value: Int): Unit = {
        buf.writeByte(Bson.NUMBER_INT)
        writeNulString(buf, name)
        buf.writeInt(value)
    }

    final private[cdriver] def writeFieldLong(buf: ChannelBuffer, name: String, value: Long): Unit = {
        buf.writeByte(Bson.NUMBER_LONG)
        writeNulString(buf, name)
        buf.writeLong(value)
    }

    final private[cdriver] def writeFieldString(buf: ChannelBuffer, name: String, value: String): Unit = {
        buf.writeByte(Bson.STRING)
        writeNulString(buf, name)
        writeLengthString(buf, value)
    }

    final private[cdriver] def writeFieldBoolean(buf: ChannelBuffer, name: String, value: Boolean): Unit = {
        buf.writeByte(Bson.BOOLEAN)
        writeNulString(buf, name)
        buf.writeByte(if (value) 1 else 0)
    }

    final private[cdriver] def writeFieldObjectId(buf: ChannelBuffer, name: String, value: ObjectId): Unit = {
        buf.writeByte(Bson.OID)
        writeNulString(buf, name)
        buf.writeInt(swapInt(value.time))
        buf.writeInt(swapInt(value.machine))
        buf.writeInt(swapInt(value.inc))
    }

    final private[cdriver] def writeFieldObject[Q](buf: ChannelBuffer, name: String, query: Q)(implicit querySupport: QueryEncoder[Q]): Unit = {
        buf.writeByte(Bson.OBJECT)
        writeNulString(buf, name)
        writeQuery(buf, query, Int.MaxValue /* max size; already checked for outer object */ )
    }

    def writeValue(buf: ChannelBuffer, name: String, bvalue: BValue): Unit = {
        buf.ensureWritableBytes(1 + name.length() + 1 + 16) // typecode + name + nul + large value size
        bvalue match {
            case v: BInt32 =>
                writeFieldInt(buf, name, v.value)
            case v: BInt64 =>
                writeFieldLong(buf, name, v.value)
            case v: BDouble =>
                buf.writeByte(Bson.NUMBER)
                writeNulString(buf, name)
                buf.writeDouble(v.value)
            case v: BObjectId =>
                writeFieldObjectId(buf, name, v.value)
            case v: BString =>
                writeFieldString(buf, name, v.value)
            case v: BObject =>
                writeFieldObject(buf, name, v)
            case v: BArray =>
                buf.writeByte(Bson.ARRAY)
                writeNulString(buf, name)
                writeArray(buf, v)
            case v: BTimestamp =>
                buf.writeByte(Bson.TIMESTAMP)
                writeNulString(buf, name)
                buf.writeInt(v.value.inc)
                buf.writeInt(v.value.time)
            case v: BISODate =>
                buf.writeByte(Bson.DATE)
                writeNulString(buf, name)
                buf.writeLong(v.value.getMillis())
            case v: BBinary =>
                buf.writeByte(Bson.BINARY)
                writeNulString(buf, name)
                val bytes = v.value.data
                buf.ensureWritableBytes(bytes.length + 5)
                buf.writeInt(bytes.length)
                buf.writeByte(BsonSubtype.toByte(v.value.subtype))
                buf.writeBytes(bytes)
            case v: BBoolean =>
                writeFieldBoolean(buf, name, v.value)
            case BNull =>
                buf.writeByte(Bson.NULL)
                writeNulString(buf, name)
            // no data on the wire for null
            case v: JObject =>
                throw new MongoException("Can't use JObject in queries: " + v)
            case v: JArray =>
                throw new MongoException("Can't use JArray in queries: " + v)
        }
    }

    private[cdriver] object BObjectDecodeSupport
        extends NettyDocumentDecoder[BObject]
        with QueryResultDecoder[BObject] {
        override final def read(buf: ChannelBuffer): BObject = {
            val len = buf.readInt()
            if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
                buf.skipBytes(len - 4)
                BObject.empty
            } else {
                val b = BObject.newBuilder

                var what = buf.readByte()
                while (what != Bson.EOO) {

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
        if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
            buf.skipBytes(len - 4)
            BArray.empty
        } else {
            val b = BArray.newBuilder

            var what = buf.readByte()
            while (what != Bson.EOO) {

                // the names in an array are just the indices, so nobody cares
                skipNulString(buf)

                b += readBValue(what, buf)

                what = buf.readByte()
            }

            b.result()
        }
    }

    private[cdriver] def skipValue(what: Byte, buf: ChannelBuffer): Unit = {
        what match {
            case Bson.NUMBER =>
                buf.skipBytes(8)
            case Bson.STRING =>
                skipLengthString(buf)
            case Bson.OBJECT =>
                skipDocument(buf)
            case Bson.ARRAY =>
                skipDocument(buf)
            case Bson.BINARY =>
                val len = buf.readInt()
                val subtype = buf.readByte()
                buf.skipBytes(len)
            case Bson.OID =>
                buf.skipBytes(12)
            case Bson.BOOLEAN =>
                buf.skipBytes(1)
            case Bson.DATE =>
                buf.skipBytes(8)
            case Bson.NULL => // nothing to skip
            case Bson.NUMBER_INT =>
                buf.skipBytes(4)
            case Bson.TIMESTAMP =>
                buf.skipBytes(8)
            case Bson.NUMBER_LONG =>
                buf.skipBytes(8)
            case Bson.UNDEFINED |
                Bson.REGEX |
                Bson.REF |
                Bson.CODE |
                Bson.SYMBOL |
                Bson.CODE_W_SCOPE |
                Bson.MINKEY |
                Bson.MAXKEY =>
                throw new MongoException("Encountered value of type " + Integer.toHexString(what) + " which is currently unsupported")
        }
    }

    private def readBValue(what: Byte, buf: ChannelBuffer): BValue = {
        what match {
            case Bson.NUMBER =>
                BDouble(buf.readDouble())
            case Bson.STRING =>
                BString(readLengthString(buf))
            case Bson.OBJECT =>
                readEntity[BObject](buf)
            case Bson.ARRAY =>
                readArray(buf)
            case Bson.BINARY =>
                val len = buf.readInt()
                val subtype = BsonSubtype.fromByte(buf.readByte()).getOrElse(BsonSubtype.GENERAL)
                val bytes = new Array[Byte](len)
                buf.readBytes(bytes)
                BBinary(Binary(bytes, subtype))
            case Bson.OID =>
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
            case Bson.BOOLEAN =>
                BBoolean(buf.readByte() != 0)
            case Bson.DATE =>
                BISODate(buf.readLong())
            case Bson.NULL =>
                BNull
            case Bson.NUMBER_INT =>
                BInt32(buf.readInt())
            case Bson.TIMESTAMP =>
                val inc = buf.readInt()
                val time = buf.readInt()
                BTimestamp(Timestamp(time, inc))
            case Bson.NUMBER_LONG =>
                BInt64(buf.readLong())
            case Bson.UNDEFINED |
                Bson.REGEX |
                Bson.REF |
                Bson.CODE |
                Bson.SYMBOL |
                Bson.CODE_W_SCOPE |
                Bson.MINKEY |
                Bson.MAXKEY =>
                throw new MongoException("Encountered value of type " + Integer.toHexString(what) + " which is currently unsupported")
        }
    }
}
