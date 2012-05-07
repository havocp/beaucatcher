package org.beaucatcher.mongo

import java.nio.charset.Charset
import org.beaucatcher.wire._
import org.beaucatcher.bson._

// All these private utility methods would need cleaning up if made public
private[beaucatcher] object CodecUtils {

    private[this] val zeroByte = 0.toByte
    private[this] val utf8Charset = Charset.forName("UTF-8");

    def swapInt(value: Int): Int = {
        (((value >> 0) & 0xff) << 24) |
            (((value >> 8) & 0xff) << 16) |
            (((value >> 16) & 0xff) << 8) |
            (((value >> 24) & 0xff) << 0)
    }

    def writeNulString(buf: EncodeBuffer, s: String): Unit = {
        val bytes = s.getBytes(utf8Charset)
        buf.ensureWritableBytes(bytes.length + 1)
        buf.writeBytes(bytes)
        buf.writeByte('\0')
    }

    def writeLengthString(buf: EncodeBuffer, s: String): Unit = {
        val bytes = s.getBytes(utf8Charset)
        buf.ensureWritableBytes(bytes.length + 1)
        buf.writeInt(bytes.length + 1) // + 1 for nul
        buf.writeBytes(bytes)
        buf.writeByte('\0')
    }

    def readNulString(buf: DecodeBuffer): String = {
        val start = buf.readerIndex
        val len = buf.bytesBefore(zeroByte)
        val bytes = new Array[Byte](len)
        buf.readBytes(bytes)
        buf.readByte() // mop up the nul
        new String(bytes, utf8Charset)
    }

    def readLengthString(buf: DecodeBuffer): String = {
        val len = buf.readInt()
        val bytes = new Array[Byte](len - 1) // -1 for nul
        buf.readBytes(bytes)
        buf.readByte() // mop up the nul
        new String(bytes, utf8Charset)
    }

    def skipNulString(buf: DecodeBuffer): Unit = {
        val len = buf.bytesBefore(zeroByte)
        buf.skipBytes(len + 1)
    }

    def skipLengthString(buf: DecodeBuffer): Unit = {
        val len = buf.readInt()
        buf.skipBytes(len)
    }

    def skipDocument(buf: DecodeBuffer): Unit = {
        val len = buf.readInt()
        buf.skipBytes(len - 4)
    }

    def writeEmptyQuery(buf: EncodeBuffer): Unit = {
        buf.ensureWritableBytes(5)
        buf.writeInt(Bson.EMPTY_DOCUMENT_LENGTH)
        buf.writeByte(zeroByte)
    }

    def writeOpenDocument(buf: EncodeBuffer): Int = {
        val start = buf.writerIndex
        buf.ensureWritableBytes(32) // min size is 5, but prealloc for efficiency
        buf.writeInt(0) // will write this later
        start
    }

    def writeCloseDocument(buf: EncodeBuffer, start: Int) = {
        buf.ensureWritableBytes(1)
        buf.writeByte('\0')
        buf.setInt(start, buf.writerIndex - start)
    }

    def writeDocument[D](buf: EncodeBuffer, doc: D, maxSize: Int)(implicit encodeSupport: DocumentEncoder[D]): Unit = {
        val start = buf.writerIndex
        encodeSupport.encode(buf, doc)
        val size = buf.writerIndex - start
        if (size > maxSize)
            throw new DocumentTooLargeMongoException("Document is too large (" + size + " bytes but the max is " + maxSize + ")")
    }

    private[this] val intStrings = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20").toArray(manifest[String])

    private[this] def arrayIndex(i: Int): String = {
        if (i < intStrings.length)
            intStrings(i)
        else
            Integer.toString(i)
    }

    def writeArrayAny(buf: EncodeBuffer, array: Seq[Any], documentFieldWriter: FieldWriter): Unit = {
        val start = buf.writerIndex
        buf.ensureWritableBytes(32) // some prealloc for efficiency
        buf.writeInt(0) // will write this later
        var i = 0
        for (element <- array) {
            writeValueAny(buf, arrayIndex(i), element, documentFieldWriter)
            i += 1
        }
        buf.ensureWritableBytes(1)
        buf.writeByte('\0')
        buf.setInt(start, buf.writerIndex - start)
    }

    def writeFieldInt(buf: EncodeBuffer, name: String, value: Int): Unit = {
        buf.writeByte(Bson.NUMBER_INT)
        writeNulString(buf, name)
        buf.writeInt(value)
    }

    def writeFieldLong(buf: EncodeBuffer, name: String, value: Long): Unit = {
        buf.writeByte(Bson.NUMBER_LONG)
        writeNulString(buf, name)
        buf.writeLong(value)
    }

    def writeFieldDouble(buf: EncodeBuffer, name: String, value: Double): Unit = {
        buf.writeByte(Bson.NUMBER)
        writeNulString(buf, name)
        buf.writeDouble(value)
    }

    def writeFieldString(buf: EncodeBuffer, name: String, value: String): Unit = {
        buf.writeByte(Bson.STRING)
        writeNulString(buf, name)
        writeLengthString(buf, value)
    }

    def writeFieldBoolean(buf: EncodeBuffer, name: String, value: Boolean): Unit = {
        buf.writeByte(Bson.BOOLEAN)
        writeNulString(buf, name)
        buf.writeByte(if (value) 1 else 0)
    }

    def writeFieldObjectId(buf: EncodeBuffer, name: String, value: ObjectId): Unit = {
        buf.writeByte(Bson.OID)
        writeNulString(buf, name)
        buf.writeInt(swapInt(value.time))
        buf.writeInt(swapInt(value.machine))
        buf.writeInt(swapInt(value.inc))
    }

    def writeFieldDocument[Q](buf: EncodeBuffer, name: String, query: Q)(implicit querySupport: DocumentEncoder[Q]): Unit = {
        buf.writeByte(Bson.OBJECT)
        writeNulString(buf, name)
        writeDocument(buf, query, Int.MaxValue /* max size; already checked for outer object */ )
    }

    def writeFieldQuery[Q](buf: EncodeBuffer, name: String, query: Q)(implicit querySupport: QueryEncoder[Q]): Unit = {
        writeFieldDocument(buf, name, query)(querySupport)
    }

    def writeFieldModifier[Q](buf: EncodeBuffer, name: String, query: Q)(implicit querySupport: ModifierEncoder[Q]): Unit = {
        writeFieldDocument(buf, name, query)(querySupport)
    }

    type FieldWriter = PartialFunction[(EncodeBuffer, String, Any), Unit]

    def writeValueAny(buf: EncodeBuffer, name: String, value: Any, documentFieldWriter: FieldWriter): Unit = {
        buf.ensureWritableBytes(1 + name.length() + 1 + 16) // typecode + name + nul + large value size
        value match {
            case null =>
                buf.writeByte(Bson.NULL)
                writeNulString(buf, name)
            // no data on the wire for null
            case v: Int =>
                writeFieldInt(buf, name, v)
            case v: Long =>
                writeFieldLong(buf, name, v)
            case v: Double =>
                writeFieldDouble(buf, name, v)
            case v: ObjectId =>
                writeFieldObjectId(buf, name, v)
            case v: String =>
                writeFieldString(buf, name, v)
            case v: Timestamp =>
                buf.writeByte(Bson.TIMESTAMP)
                writeNulString(buf, name)
                buf.writeInt(v.inc)
                buf.writeInt(v.time)
            case v: java.util.Date =>
                buf.writeByte(Bson.DATE)
                writeNulString(buf, name)
                buf.writeLong(v.getTime)
            case v: Binary =>
                buf.writeByte(Bson.BINARY)
                writeNulString(buf, name)
                val bytes = v.data
                buf.ensureWritableBytes(bytes.length + 5)
                buf.writeInt(bytes.length)
                buf.writeByte(BsonSubtype.toByte(v.subtype))
                buf.writeBytes(bytes)
            case v: Boolean =>
                writeFieldBoolean(buf, name, v)
            // handles document objects
            case v if documentFieldWriter.isDefinedAt((buf, name, v)) =>
                documentFieldWriter.apply((buf, name, v))
            // handle arrays after documents, in case the
            // documentFieldWriter wants to try to special-case
            // certain Seq
            case v: Seq[_] =>
                buf.writeByte(Bson.ARRAY)
                writeNulString(buf, name)
                writeArrayAny(buf, v, documentFieldWriter)
            case unknown =>
                throw new BugInSomethingMongoException("Value is not a supported type for encoding: " +
                    unknown.getClass.getSimpleName + ": " + unknown)
        }
    }

    def writeQuery[Q](buf: EncodeBuffer, query: Q, maxSize: Int)(implicit querySupport: QueryEncoder[Q]): Unit = {
        writeDocument(buf, query, maxSize)
    }

    def writeUpdateQuery[Q](buf: EncodeBuffer, query: Q, maxSize: Int)(implicit querySupport: UpdateQueryEncoder[Q]): Unit = {
        writeDocument(buf, query, maxSize)
    }

    def writeModifier[E](buf: EncodeBuffer, entity: E, maxSize: Int)(implicit entitySupport: ModifierEncoder[E]): Unit = {
        writeDocument(buf, entity, maxSize)
    }

    def writeUpsert[E](buf: EncodeBuffer, entity: E, maxSize: Int)(implicit entitySupport: UpsertEncoder[E]): Unit = {
        writeDocument(buf, entity, maxSize)
    }

    def writeIdField[I](buf: EncodeBuffer, name: String, id: I)(implicit idEncoder: IdEncoder[I]): Unit = {
        idEncoder.encodeField(buf, name, id)
    }

    def readDocument[E](buf: DecodeBuffer)(implicit entitySupport: DocumentDecoder[E]): E = {
        entitySupport.decode(buf)
    }

    def readEntity[E](buf: DecodeBuffer)(implicit entitySupport: QueryResultDecoder[E]): E = {
        readDocument(buf)
    }

    def readValue[V](what: Byte, buf: DecodeBuffer)(implicit valueDecoder: ValueDecoder[V]): V = {
        valueDecoder.decode(what, buf)
    }

    def skipValue(what: Byte, buf: DecodeBuffer): Unit = {
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
                // TODO
                throw new MongoException("Encountered value of type " + Integer.toHexString(what) + " which is currently unsupported")
        }
    }

    def readArrayValues[E](buf: DecodeBuffer)(implicit elementDecoder: ValueDecoder[E]): Seq[E] = {
        val b = Seq.newBuilder[E]
        decodeArrayForeach(buf, { (what, buf) =>
            b += elementDecoder.decode(what, buf)
        })
        b.result()
    }

    def readAny[E](what: Byte, buf: DecodeBuffer)(implicit nestedDecoder: DocumentDecoder[E]): Any = {
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
                new java.util.Date(buf.readLong())
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
                readArrayAny[E](buf)
            case Bson.OBJECT =>
                readDocument[E](buf)
            case Bson.BINARY =>
                val len = buf.readInt()
                val subtype = BsonSubtype.fromByte(buf.readByte()).getOrElse(BsonSubtype.GENERAL)
                val bytes = new Array[Byte](len)
                buf.readBytes(bytes)
                Binary(bytes, subtype)
            case Bson.UNDEFINED |
                Bson.REGEX |
                Bson.REF |
                Bson.CODE |
                Bson.SYMBOL |
                Bson.CODE_W_SCOPE |
                Bson.MINKEY |
                Bson.MAXKEY |
                _ =>
                // TODO
                throw new MongoException("Encountered value of type " + Integer.toHexString(what) + " which is currently unsupported")
        }
    }

    private def readArrayAny[E](buf: DecodeBuffer)(implicit nestedDecoder: DocumentDecoder[E]): Seq[Any] = {
        val b = Seq.newBuilder[Any]
        decodeArrayForeach(buf, { (what, buf) =>
            b += readAny(what, buf)
        })
        b.result()
    }

    def decodeDocumentForeach[T](buf: DecodeBuffer, func: (Byte, String, DecodeBuffer) => Unit): Unit = {
        val len = buf.readInt()
        if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
            buf.skipBytes(len - 4)
        } else {
            var what = buf.readByte()
            while (what != Bson.EOO) {

                val name = readNulString(buf)

                func(what, name, buf)

                what = buf.readByte()
            }
        }
    }

    def decodeDocumentIterator[T](orig: DecodeBuffer, func: (Byte, String, DecodeBuffer) => T): Iterator[(String, T)] = {
        val len = orig.readInt()
        if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
            orig.skipBytes(len - 4)
            Iterator.empty
        } else {
            // In order to allow the iterator to be lazy, we have to take a slice
            // so we can synchronously skip bytes in the original DecodeBuffer
            val buf = orig.slice(orig.readerIndex, len - 4)
            orig.skipBytes(len - 4)

            new Iterator[(String, T)]() {
                private var nextWhat = buf.readByte()

                override def hasNext = nextWhat != Bson.EOO

                override def next() = {
                    if (nextWhat == Bson.EOO)
                        throw new NoSuchElementException("Reached end of BSON document")
                    val name = readNulString(buf)
                    val t = func(nextWhat, name, buf)

                    nextWhat = buf.readByte()
                    (name -> t)
                }
            }
        }
    }

    def decodeArrayForeach[T](buf: DecodeBuffer, func: (Byte, DecodeBuffer) => Unit): Unit = {
        val len = buf.readInt()
        if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
            buf.skipBytes(len - 4)
        } else {
            var what = buf.readByte()
            while (what != Bson.EOO) {

                // the names in an array are just the indices, so nobody cares
                skipNulString(buf)

                func(what, buf)

                what = buf.readByte()
            }
        }
    }
}
