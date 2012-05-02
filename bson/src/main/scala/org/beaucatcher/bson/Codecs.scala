package org.beaucatcher.bson

import org.beaucatcher.mongo._
import org.beaucatcher.wire._

object Codecs {
    import CodecUtils._

    implicit def stringIdEncoder : IdEncoder[String] =
        StringIdEncoder

    implicit def intIdEncoder : IdEncoder[Int] =
        IntIdEncoder

    implicit def longIdEncoder : IdEncoder[Long] =
        LongIdEncoder

    implicit def objectIdIdEncoder : IdEncoder[ObjectId] =
        ObjectIdIdEncoder

    implicit def bvalueValueDecoder : ValueDecoder[BValue] =
        BValueValueDecoder

    // it isn't safe to make this implicit since it would always apply
    def anyValueDecoder : ValueDecoder[Any] =
        AnyValueDecoder

    implicit def bobjectQueryEncoder : QueryEncoder[BObject] =
        BObjectUnmodifiedEncoder

    implicit def bobjectQueryResultDecoder : QueryResultDecoder[BObject] =
        BObjectDecoder

    implicit def bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject] =
        BObjectOnlyIdEncoder

    implicit def bobjectModifierEncoder : ModifierEncoder[BObject] =
        BObjectWithoutIdEncoder

    implicit def bobjectUpsertEncoder : UpsertEncoder[BObject] =
        BObjectUnmodifiedEncoder

    def newBObjectCodecSet[IdType : IdEncoder]() : CollectionCodecSet[BObject, BObject, IdType, BValue] =
        CodecSets.newBObjectCodecSet()

    def newCaseClassCodecSet[EntityType <: Product : Manifest, IdType : IdEncoder]() : CollectionCodecSet[BObject, EntityType, IdType, Any] =
        CodecSets.newCaseClassCodecSet()

    private[beaucatcher] def swapInt(value : Int) : Int = {
        (((value >> 0) & 0xff) << 24) |
            (((value >> 8) & 0xff) << 16) |
            (((value >> 16) & 0xff) << 8) |
            (((value >> 24) & 0xff) << 0)
    }

    private[beaucatcher] def writeOpenDocument(buf : EncodeBuffer) : Int = {
        val start = buf.writerIndex
        buf.ensureWritableBytes(32) // min size is 5, but prealloc for efficiency
        buf.writeInt(0) // will write this later
        start
    }

    private[beaucatcher] def writeCloseDocument(buf : EncodeBuffer, start : Int) = {
        buf.ensureWritableBytes(1)
        buf.writeByte('\0')
        buf.setInt(start, buf.writerIndex - start)
    }

    private[beaucatcher] object BObjectUnmodifiedEncoder
        extends QueryEncoder[BObject]
        with UpsertEncoder[BObject] {
        override def encode(buf : EncodeBuffer, t : BObject) : Unit = {
            val start = writeOpenDocument(buf)

            for (field <- t.value) {
                writeValue(buf, field._1, field._2)
            }

            writeCloseDocument(buf, start)
        }
    }

    private[beaucatcher] object BObjectWithoutIdEncoder
        extends ModifierEncoder[BObject] {
        override def encode(buf : EncodeBuffer, o : BObject) : Unit = {
            BObjectUnmodifiedEncoder.encode(buf, o - "_id")
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object BObjectOnlyIdEncoder
        extends UpdateQueryEncoder[BObject] {
        override def encode(buf : EncodeBuffer, o : BObject) : Unit = {
            val idQuery = BObject("_id" -> o.getOrElse("_id", throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)")))
            BObjectUnmodifiedEncoder.encode(buf, idQuery)
        }
    }

    private[beaucatcher] object StringIdEncoder
        extends IdEncoder[String] {
        override def encodeField(buf : EncodeBuffer, name : String, value : String) : Unit = {
            writeFieldString(buf, name, value)
        }
    }

    private[beaucatcher] object ObjectIdIdEncoder
        extends IdEncoder[ObjectId] {
        override def encodeField(buf : EncodeBuffer, name : String, value : ObjectId) : Unit = {
            writeFieldObjectId(buf, name, value)
        }
    }

    private[beaucatcher] object IntIdEncoder
        extends IdEncoder[Int] {
        override def encodeField(buf : EncodeBuffer, name : String, value : Int) : Unit = {
            writeFieldInt(buf, name, value)
        }
    }

    private[beaucatcher] object LongIdEncoder
        extends IdEncoder[Long] {
        override def encodeField(buf : EncodeBuffer, name : String, value : Long) : Unit = {
            writeFieldLong(buf, name, value)
        }
    }

    private[beaucatcher] object BValueValueDecoder
        extends ValueDecoder[BValue] {
        override def decode(code : Byte, buf : DecodeBuffer) : BValue = {
            readBValue(code, buf)
        }
    }

    private[beaucatcher] object AnyValueDecoder
        extends ValueDecoder[Any] {

        implicit val errorDecoder = new QueryResultDecoder[Unit] {
            override def decode(buf : DecodeBuffer) : Unit = {
                throw new MongoException("Cannot decode an object value here because it's unknown how to represent the object in Scala")
            }
        }

        override def decode(code : Byte, buf : DecodeBuffer) : Any = {
            readAny(code, buf)(errorDecoder)
        }
    }

    private[this] val intStrings = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20").toArray(manifest[String])

    private[this] def arrayIndex(i : Int) : String = {
        if (i < intStrings.length)
            intStrings(i)
        else
            Integer.toString(i)
    }

    private def writeArray(buf : EncodeBuffer, array : BArray) : Unit = {
        val start = buf.writerIndex
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

    private[beaucatcher] def writeFieldInt(buf : EncodeBuffer, name : String, value : Int) : Unit = {
        buf.writeByte(Bson.NUMBER_INT)
        writeNulString(buf, name)
        buf.writeInt(value)
    }

    private[beaucatcher] def writeFieldLong(buf : EncodeBuffer, name : String, value : Long) : Unit = {
        buf.writeByte(Bson.NUMBER_LONG)
        writeNulString(buf, name)
        buf.writeLong(value)
    }

    private[beaucatcher] def writeFieldString(buf : EncodeBuffer, name : String, value : String) : Unit = {
        buf.writeByte(Bson.STRING)
        writeNulString(buf, name)
        writeLengthString(buf, value)
    }

    private[beaucatcher] def writeFieldBoolean(buf : EncodeBuffer, name : String, value : Boolean) : Unit = {
        buf.writeByte(Bson.BOOLEAN)
        writeNulString(buf, name)
        buf.writeByte(if (value) 1 else 0)
    }

    private[beaucatcher] def writeFieldObjectId(buf : EncodeBuffer, name : String, value : ObjectId) : Unit = {
        buf.writeByte(Bson.OID)
        writeNulString(buf, name)
        buf.writeInt(swapInt(value.time))
        buf.writeInt(swapInt(value.machine))
        buf.writeInt(swapInt(value.inc))
    }

    private[beaucatcher] def writeFieldDocument[Q](buf : EncodeBuffer, name : String, query : Q)(implicit querySupport : DocumentEncoder[Q]) : Unit = {
        buf.writeByte(Bson.OBJECT)
        writeNulString(buf, name)
        writeDocument(buf, query, Int.MaxValue /* max size; already checked for outer object */ )
    }

    private[beaucatcher] def writeFieldQuery[Q](buf : EncodeBuffer, name : String, query : Q)(implicit querySupport : QueryEncoder[Q]) : Unit = {
        writeFieldDocument(buf, name, query)(querySupport)
    }

    private[beaucatcher] def writeFieldModifier[Q](buf : EncodeBuffer, name : String, query : Q)(implicit querySupport : ModifierEncoder[Q]) : Unit = {
        writeFieldDocument(buf, name, query)(querySupport)
    }

    private[beaucatcher] def writeValue(buf : EncodeBuffer, name : String, bvalue : BValue) : Unit = {
        buf.ensureWritableBytes(1 + name.length() + 1 + 16) // typecode + name + nul + large value size
        bvalue match {
            case v : BInt32 =>
                writeFieldInt(buf, name, v.value)
            case v : BInt64 =>
                writeFieldLong(buf, name, v.value)
            case v : BDouble =>
                buf.writeByte(Bson.NUMBER)
                writeNulString(buf, name)
                buf.writeDouble(v.value)
            case v : BObjectId =>
                writeFieldObjectId(buf, name, v.value)
            case v : BString =>
                writeFieldString(buf, name, v.value)
            case v : BObject =>
                writeFieldQuery(buf, name, v)
            case v : BArray =>
                buf.writeByte(Bson.ARRAY)
                writeNulString(buf, name)
                writeArray(buf, v)
            case v : BTimestamp =>
                buf.writeByte(Bson.TIMESTAMP)
                writeNulString(buf, name)
                buf.writeInt(v.value.inc)
                buf.writeInt(v.value.time)
            case v : BISODate =>
                buf.writeByte(Bson.DATE)
                writeNulString(buf, name)
                buf.writeLong(v.value.getTime)
            case v : BBinary =>
                buf.writeByte(Bson.BINARY)
                writeNulString(buf, name)
                val bytes = v.value.data
                buf.ensureWritableBytes(bytes.length + 5)
                buf.writeInt(bytes.length)
                buf.writeByte(BsonSubtype.toByte(v.value.subtype))
                buf.writeBytes(bytes)
            case v : BBoolean =>
                writeFieldBoolean(buf, name, v.value)
            case BNull =>
                buf.writeByte(Bson.NULL)
                writeNulString(buf, name)
            // no data on the wire for null
            case v : JObject =>
                throw new MongoException("Can't use JObject in queries: " + v)
            case v : JArray =>
                throw new MongoException("Can't use JArray in queries: " + v)
        }
    }

    private[beaucatcher] object BObjectDecoder
        extends QueryResultDecoder[BObject] {
        override def decode(buf : DecodeBuffer) : BObject = {
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

    private def readArray(buf : DecodeBuffer) : BArray = {
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

    private[beaucatcher] def skipValue(what : Byte, buf : DecodeBuffer) : Unit = {
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

    private def readBValue(what : Byte, buf : DecodeBuffer) : BValue = {
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

    private def readArrayAny[E](buf : DecodeBuffer)(implicit nestedDecoder : QueryResultDecoder[E]) : Seq[Any] = {
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

    private[beaucatcher] def readArrayValues[E](buf : DecodeBuffer)(implicit elementDecoder : ValueDecoder[E]) : Seq[E] = {
        val len = buf.readInt()
        if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
            buf.skipBytes(len - 4)
            Seq.empty
        } else {
            val b = Seq.newBuilder[E]

            var what = buf.readByte()
            while (what != Bson.EOO) {

                // the names in an array are just the indices, so nobody cares
                skipNulString(buf)

                b += elementDecoder.decode(what, buf)

                what = buf.readByte()
            }

            b.result()
        }
    }

    private[beaucatcher] def readAny[E](what : Byte, buf : DecodeBuffer)(implicit nestedDecoder : QueryResultDecoder[E]) : Any = {
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
                readArrayAny(buf)
            case Bson.OBJECT =>
                readEntity[E](buf)
            case Bson.BINARY |
                Bson.UNDEFINED |
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
}
