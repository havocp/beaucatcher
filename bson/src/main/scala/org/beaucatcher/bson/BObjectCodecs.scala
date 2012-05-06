package org.beaucatcher.bson

import org.beaucatcher.mongo._
import org.beaucatcher.wire._

object BObjectCodecs extends IdEncoders with ValueDecoders {
    import CodecUtils._

    implicit def bvalueValueDecoder : ValueDecoder[BValue] =
        BValueValueDecoder

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

    private[beaucatcher] object BObjectUnmodifiedEncoder
        extends QueryEncoder[BObject]
        with UpsertEncoder[BObject] {
        override def encode(buf : EncodeBuffer, t : BObject) : Unit = {
            val start = writeOpenDocument(buf)

            for (field <- t.value) {
                writeBValue(buf, field._1, field._2)
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

    private[beaucatcher] object BValueValueDecoder
        extends ValueDecoder[BValue] {
        override def decode(code : Byte, buf : DecodeBuffer) : BValue = {
            readBValue(code, buf)
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

    private def writeBArray(buf : EncodeBuffer, array : BArray) : Unit = {
        val start = buf.writerIndex
        buf.ensureWritableBytes(32) // some prealloc for efficiency
        buf.writeInt(0) // will write this later
        var i = 0
        for (element <- array.value) {
            writeBValue(buf, arrayIndex(i), element)
            i += 1
        }
        buf.ensureWritableBytes(1)
        buf.writeByte('\0')
        buf.setInt(start, buf.writerIndex - start)
    }

    private[beaucatcher] def writeBValue(buf : EncodeBuffer, name : String, bvalue : BValue) : Unit = {
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
                writeBArray(buf, v)
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
            val b = BObject.newBuilder
            decodeDocumentForeach(buf, { (what, name, buf) =>
                b += (name -> readBValue(what, buf))
            })

            b.result()
        }
    }

    private def readBArray(buf : DecodeBuffer) : BArray = {
        val b = BArray.newBuilder
        decodeArrayForeach(buf, { (what, buf) =>
            b += readBValue(what, buf)
        })

        b.result()
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
                readBArray(buf)
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
