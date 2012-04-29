package org.beaucatcher.mongo.cdriver

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.channel.netty._
import org.jboss.netty.buffer._
import org.jboss.netty.buffer.ChannelBuffers._
import java.nio.ByteOrder

private[cdriver] class RawEncoded {
    import Codecs._

    private val buf: ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 32)

    private val start = writeOpenDocument(buf)
    var closed = false

    private def ensureFieldBytes(name: String): Unit = {
        if (closed)
            throw new IllegalStateException("bug: wrote to closed RawEncode")
        buf.ensureWritableBytes(1 + name.length() + 1 + 16) // typecode + name + nul + large value size
    }

    // we use this to write "_id" at the moment so limited
    // set of value types lets us limp along.
    def writeFieldAny(name: String, value: Any): Unit = {
        value match {
            case s: String =>
                writeField(name, s)
            case oid: ObjectId =>
                writeField(name, oid)
            case i: Int =>
                writeField(name, i)
            case l: Long =>
                writeField(name, l)
            case f =>
                throw new MongoException("For key " + name + " unsupported value type " + value)
        }
    }

    def writeFieldIntOption(name: String, value: Option[Int]): Unit = {
        value.foreach({ v => writeField(name, v) })
    }

    def writeField(name: String, value: Int): Unit = {
        ensureFieldBytes(name)
        writeFieldInt(buf, name, value)
    }

    def writeFieldLongOption(name: String, value: Option[Long]): Unit = {
        value.foreach({ v => writeField(name, v) })
    }

    def writeField(name: String, value: Long): Unit = {
        ensureFieldBytes(name)
        writeFieldLong(buf, name, value)
    }

    def writeFieldStringOption(name: String, value: Option[String]): Unit = {
        value.foreach({ v => writeField(name, v) })
    }

    def writeField(name: String, value: String): Unit = {
        ensureFieldBytes(name)
        writeFieldString(buf, name, value)
    }

    def writeField(name: String, value: ObjectId): Unit = {
        ensureFieldBytes(name)
        writeFieldObjectId(buf, name, value)
    }

    def writeField[Q](name: String, query: Option[Q])(implicit querySupport: QueryEncoder[Q]): Unit = {
        query.foreach({ v => writeField(name, v) })
    }

    def writeField[Q](name: String, query: Q)(implicit querySupport: QueryEncoder[Q]): Unit = {
        ensureFieldBytes(name)
        writeFieldObject(buf, name, query)
    }

    def writeField(name: String, value: Boolean): Unit = {
        ensureFieldBytes(name)
        writeFieldBoolean(buf, name, value)
    }

    def writeFields(fields: Fields): Unit = {
        for (i <- fields.included) {
            writeField(i, 1)
        }
        for (e <- fields.excluded) {
            writeField(e, 0)
        }
    }

    def writeField(name: String, fieldsOption: Option[Fields]): Unit = {
        fieldsOption.foreach({ fields =>
            val raw = RawEncoded()
            raw.writeFields(fields)
            writeField(name, raw)
        })
    }

    def close(): Unit = {
        if (closed)
            throw new IllegalStateException("bug: closed RawEncode twice")
        writeCloseDocument(buf, start)
        closed = true
    }

    def writeTo(target: ChannelBuffer): Unit = {
        if (!closed)
            close()
        target.writeBytes(buf, 0, buf.writerIndex())
    }
}

object RawEncoded {
    private object RawEncodeSupport
        extends NettyDocumentEncoder[RawEncoded]
        with QueryEncoder[RawEncoded]
        with EntityEncodeSupport[RawEncoded] {
        override final def write(buf: ChannelBuffer, t: RawEncoded): Unit = {
            t.writeTo(buf)
        }
    }

    implicit def rawQueryEncoder: QueryEncoder[RawEncoded] = RawEncodeSupport
    implicit def rawEntityEncodeSupport: EntityEncodeSupport[RawEncoded] = RawEncodeSupport

    def apply(): RawEncoded = new RawEncoded()

    private object FieldsEncodeSupport
        extends NettyDocumentEncoder[Fields]
        with QueryEncoder[Fields] {
        override final def write(buf: ChannelBuffer, t: Fields): Unit = {
            val raw = RawEncoded()
            raw.writeFields(t)
            raw.writeTo(buf)
        }
    }

    implicit def fieldsQueryEncoder: QueryEncoder[Fields] = FieldsEncodeSupport
}

private[cdriver] class RawDecoded {
    import Codecs._

    var fields: Map[String, Any] = Map.empty
}

object RawDecoded {
    import org.beaucatcher.wire._
    import Codecs._

    private class RawDecodeSupport[NestedEntityType](val needed: Seq[String])(implicit val nestedDecodeSupport: QueryResultDecoder[NestedEntityType])
        extends NettyDocumentDecoder[RawDecoded]
        with QueryResultDecoder[RawDecoded] {

        private def readArray(buf: ChannelBuffer): Seq[Any] = {
            val len = buf.readInt()
            if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
                buf.skipBytes(len - 4)
                Seq.empty
            } else {
                val b = Seq.newBuilder[Any]

                var what = buf.readByte()
                while (what != Bson.EOO) {

                    // the names in an array are just the indices, so nobody cares
                    skipNulString(buf)

                    b += readAny(what, buf)

                    what = buf.readByte()
                }

                b.result()
            }
        }

        private def readAny(what: Byte, buf: ChannelBuffer): Any = {
            what match {
                case Bson.NUMBER =>
                    buf.readDouble()
                case Bson.STRING =>
                    readLengthString(buf)
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
                    ObjectId(time, machine, inc)
                case Bson.BOOLEAN =>
                    buf.readByte() != 0
                case Bson.DATE =>
                    buf.readLong()
                case Bson.NULL =>
                    null
                case Bson.NUMBER_INT =>
                    buf.readInt()
                case Bson.TIMESTAMP =>
                    val inc = buf.readInt()
                    val time = buf.readInt()
                    Timestamp(time, inc)
                case Bson.NUMBER_LONG =>
                    buf.readLong()
                case Bson.ARRAY =>
                    readArray(buf)
                case Bson.OBJECT =>
                    readEntity[NestedEntityType](buf)
                case Bson.BINARY |
                    Bson.UNDEFINED |
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

        override final def read(buf: ChannelBuffer): RawDecoded = {
            val raw = RawDecoded()
            val len = buf.readInt()
            var what = buf.readByte()
            while (what != Bson.EOO) {
                val name = readNulString(buf)

                if (needed.contains(name)) {
                    raw.fields += (name -> readAny(what, buf))
                } else {
                    skipValue(what, buf)
                }

                what = buf.readByte()
            }

            raw
        }
    }

    def rawQueryResultDecoder[NestedEntityType](needed: Seq[String])(implicit nestedSupport: QueryResultDecoder[NestedEntityType]): QueryResultDecoder[RawDecoded] =
        new RawDecodeSupport(needed)

    def apply(): RawDecoded = new RawDecoded()
}
