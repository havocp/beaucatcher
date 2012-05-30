package org.beaucatcher.bobject

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.wire._

object BObjectCodecs extends IdEncoders with ValueDecoders {
    import CodecUtils._

    // ObjectBase is unfortunately invariant in its type params
    // so really this basically breaks
    private type BOrJObject = ObjectBase[BValue, Map[String, BValue]]
    private type BOrJArray = ArrayBase[BValue]

    // can't be implicit since it would always apply
    // we also offer "bvalueValueDecoder" that
    // will decode docs as BObject, so here we
    // decode them as plain maps.
    def anyValueDecoder: ValueDecoder[Any] =
        MapCodecs.anyValueDecoder

    implicit def bvalueValueDecoder: ValueDecoder[BValue] =
        BValueValueDecoder

    implicit def bobjectQueryEncoder: QueryEncoder[BObject] =
        BObjectUnmodifiedEncoder

    implicit def bobjectQueryResultDecoder: QueryResultDecoder[BObject] =
        BObjectDecoder

    implicit def bobjectUpdateQueryEncoder: UpdateQueryEncoder[BObject] =
        BObjectOnlyIdEncoder

    implicit def bobjectModifierEncoder: ModifierEncoder[BObject] =
        BObjectWithoutIdEncoder

    implicit def bobjectUpsertEncoder: UpsertEncoder[BObject] =
        BObjectUnmodifiedEncoder

    private def unwrapIterator(bobj: BOrJObject): Iterator[(String, Any)] = {
        // note: avoid bobj.unwrapped here because it creates an unordered map
        // while BObject is ordered
        bobj.value.map(field => (field._1, unwrap(field._2))).iterator
    }

    // convert to Seq for arrays and Iterator for objects
    // as expected by encodeIterator etc.
    private def unwrap(bvalue: BValue): Any = {
        bvalue match {
            case v: ArrayBase[_] =>
                v.value.map({ e => unwrap(e) }).toSeq: Seq[Any]
            case v: ObjectBase[_, _] =>
                unwrapIterator(v.asInstanceOf[BOrJObject])
            case v: BValue =>
                // handles strings and ints and stuff
                v.unwrapped.asInstanceOf[AnyRef]
        }
    }

    private def wrapIterator(value: Iterator[(String, Any)]): BObject = {
        BObject(value.map(kv => (kv._1, wrap(kv._2))).toList)
    }

    private def wrap(value: Any): BValue = {
        value match {
            // null would otherwise match Seq and Iterator
            case null =>
                BValue.wrap(null)
            case s: Seq[_] =>
                BArray(s.map(wrap(_)))
            case i: Iterator[_] =>
                wrapIterator(i.asInstanceOf[Iterator[(String, Any)]])
            case v =>
                // handles strings and ints and stuff
                BValue.wrap(v)
        }
    }

    private[beaucatcher] object BObjectUnmodifiedEncoder
        extends QueryEncoder[BOrJObject]
        with UpsertEncoder[BOrJObject] {
        override def encode(buf: EncodeBuffer, t: BOrJObject): Unit = {
            val start = writeOpenDocument(buf)

            for (field <- t.value) {
                writeBValue(buf, field._1, field._2)
            }

            writeCloseDocument(buf, start)
        }

        override def encodeIterator(t: BOrJObject): Iterator[(String, Any)] = {
            unwrapIterator(t)
        }
    }

    private[beaucatcher] object BObjectWithoutIdEncoder
        extends ModifierEncoder[BObject] {
        override def encode(buf: EncodeBuffer, o: BObject): Unit = {
            BObjectUnmodifiedEncoder.encode(buf, o - "_id")
        }

        override def encodeIterator(t: BObject): Iterator[(String, Any)] = {
            unwrapIterator(t - "_id")
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object BObjectOnlyIdEncoder
        extends IteratorBasedDocumentEncoder[BObject]
        with UpdateQueryEncoder[BObject] {
        override def encodeIterator(o: BObject): Iterator[(String, Any)] = {
            val id = o.getOrElse("_id", throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)"))
            Iterator("_id" -> unwrap(id))
        }
    }

    private[beaucatcher] object BValueValueDecoder
        extends ValueDecoder[BValue] {
        override def decode(code: Byte, buf: DecodeBuffer): BValue = {
            readBValue(code, buf)
        }

        override def decodeAny(value: Any): BValue = {
            wrap(value)
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

    private def writeBArray(buf: EncodeBuffer, array: BOrJArray): Unit = {
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

    private[beaucatcher] def writeBValue(buf: EncodeBuffer, name: String, bvalue: BValue): Unit = {
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
            case v: ObjectBase[_, _] =>
                writeFieldQuery(buf, name, v.asInstanceOf[BOrJObject])(BObjectUnmodifiedEncoder)
            case v: ArrayBase[_] =>
                buf.writeByte(Bson.ARRAY)
                writeNulString(buf, name)
                writeBArray(buf, v.asInstanceOf[BOrJArray])
            case v: BTimestamp =>
                buf.writeByte(Bson.TIMESTAMP)
                writeNulString(buf, name)
                buf.writeInt(v.value.inc)
                buf.writeInt(v.value.time)
            case v: BISODate =>
                buf.writeByte(Bson.DATE)
                writeNulString(buf, name)
                buf.writeLong(v.value.getTime)
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
        }
    }

    private[beaucatcher] object BObjectDecoder
        extends QueryResultDecoder[BObject] {
        override def decode(buf: DecodeBuffer): BObject = {
            val b = BObject.newBuilder
            decodeDocumentForeach(buf, { (what, name, buf) =>
                b += (name -> readBValue(what, buf))
            })

            b.result()
        }

        override def decodeIterator(iterator: Iterator[(String, Any)]): BObject = {
            wrapIterator(iterator)
        }
    }

    private def readBArray(buf: DecodeBuffer): BArray = {
        val b = BArray.newBuilder
        decodeArrayForeach(buf, { (what, buf) =>
            b += readBValue(what, buf)
        })

        b.result()
    }

    private def readBValue(what: Byte, buf: DecodeBuffer): BValue = {
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

trait CollectionCodecSetEntityCodecsBObject extends CollectionCodecSetEntityCodecs[BObject] {
    self: CollectionCodecSet[_, BObject, BObject, _, _] =>

    override implicit def collectionQueryResultDecoder: QueryResultDecoder[BObject] =
        BObjectCodecs.bobjectQueryResultDecoder
    override implicit def collectionModifierEncoderEntity: ModifierEncoder[BObject] =
        BObjectCodecs.bobjectModifierEncoder
    override implicit def collectionUpdateQueryEncoder: UpdateQueryEncoder[BObject] =
        BObjectCodecs.bobjectUpdateQueryEncoder
    override implicit def collectionUpsertEncoder: UpsertEncoder[BObject] =
        BObjectCodecs.bobjectUpsertEncoder
}

trait CollectionCodecSetQueryEncodersBObject[QueryType <: BObject] extends CollectionCodecSetQueryEncoders[QueryType] {
    self: CollectionCodecSet[QueryType, _, _, _, _] =>
    override implicit def collectionQueryEncoder: QueryEncoder[QueryType] =
        BObjectCodecs.bobjectQueryEncoder
    override implicit def collectionModifierEncoderQuery: ModifierEncoder[QueryType] =
        BObjectCodecs.bobjectModifierEncoder
}

trait CollectionCodecSetValueDecoderBValue extends CollectionCodecSetValueDecoder[BValue] {
    self: CollectionCodecSet[_, _, _, _, BValue] =>

    override implicit def collectionValueDecoder: ValueDecoder[BValue] =
        BObjectCodecs.bvalueValueDecoder
}

trait CollectionCodecSetEntityBObject[-QueryType, -IdType]
    extends CollectionCodecSetValueDecoderBValue
    with CollectionCodecSetEntityCodecsBObject {
    self: CollectionCodecSet[QueryType, BObject, BObject, IdType, BValue] =>
}

object CollectionCodecSetEntityBObject {
    private class CollectionCodecSetEntityBObjectImpl[-QueryType, -IdType]()(implicit override val collectionQueryEncoder: QueryEncoder[QueryType],
        override val collectionModifierEncoderQuery: ModifierEncoder[QueryType],
        override val collectionIdEncoder: IdEncoder[IdType])
        extends CollectionCodecSet[QueryType, BObject, BObject, IdType, BValue]
        with CollectionCodecSetEntityBObject[QueryType, IdType] {

    }

    def apply[QueryType: QueryEncoder: ModifierEncoder, IdType: IdEncoder](): CollectionCodecSet[QueryType, BObject, BObject, IdType, BValue] = {
        new CollectionCodecSetEntityBObjectImpl[QueryType, IdType]()
    }
}

trait CollectionCodecSetBObject[-IdType]
    extends CollectionCodecSetQueryEncodersBObject[BObject]
    with CollectionCodecSetEntityBObject[BObject, IdType]
    with CollectionCodecSetEntityCodecsBObject {
    self: CollectionCodecSet[BObject, BObject, BObject, IdType, BValue] =>
}

object CollectionCodecSetBObject {
    private class CollectionCodecSetBObjectImpl[-IdType]()(implicit override val collectionIdEncoder: IdEncoder[IdType])
        extends CollectionCodecSet[BObject, BObject, BObject, IdType, BValue]
        with CollectionCodecSetBObject[IdType] {

    }

    def apply[IdType]()(implicit idEncoder: IdEncoder[IdType]): CollectionCodecSet[BObject, BObject, BObject, IdType, BValue] = {
        new CollectionCodecSetBObjectImpl[IdType]()
    }
}
