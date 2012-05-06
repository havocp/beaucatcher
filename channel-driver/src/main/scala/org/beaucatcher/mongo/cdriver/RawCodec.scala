package org.beaucatcher.mongo.cdriver

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.channel._
import org.beaucatcher.wire._
import java.nio.ByteOrder

private[cdriver] trait BugIfDecoded

private[cdriver] object BugIfDecoded {

    implicit lazy val bugIfDecodedDecoder = new QueryResultDecoder[BugIfDecoded] {
        override def decode(buf: DecodeBuffer): BugIfDecoded = {
            throw new BugInSomethingMongoException("Wasn't expecting to try to decode an object value here")
        }
    }

}

private[cdriver] case class RawBufferDecoded(buf: DecodeBuffer)

private[cdriver] object RawBufferDecoded {

    implicit lazy val rawBufferDecoder = new QueryResultDecoder[RawBufferDecoded] {
        override def decode(buf: DecodeBuffer): RawBufferDecoded = {
            val len = buf.readInt()
            val slice = buf.slice(buf.readerIndex - 4, len)
            buf.skipBytes(len - 4)
            RawBufferDecoded(slice)
        }
    }

    lazy val rawBufferValueDecoder = new ValueDecoder[RawBufferDecoded] {
        override def decode(what: Byte, buf: DecodeBuffer): RawBufferDecoded = {
            what match {
                case Bson.OBJECT | Bson.ARRAY =>
                    rawBufferDecoder.decode(buf)
                case _ =>
                    throw new BugInSomethingMongoException("Unexpected value type: " + what)
            }
        }
    }
}

private[cdriver] class RawEncoded(val backend: ChannelBackend) {
    import CodecUtils._

    private val buf: EncodeBuffer = backend.newDynamicEncodeBuffer(32)

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
        writeFieldQuery(buf, name, query)
    }

    def writeFieldAsModifier[M](name: String, query: Option[M])(implicit modifierEncoder: ModifierEncoder[M]): Unit = {
        query.foreach({ v => writeFieldAsModifier(name, v) })
    }

    def writeFieldAsModifier[M](name: String, query: M)(implicit modifierEncoder: ModifierEncoder[M]): Unit = {
        ensureFieldBytes(name)
        writeFieldModifier(buf, name, query)
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
            val raw = RawEncoded(backend)
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

    def writeTo(target: EncodeBuffer): Unit = {
        if (!closed)
            close()
        target.writeBuffer(buf)
    }
}

object RawEncoded {
    private object RawEncodeSupport
        extends QueryEncoder[RawEncoded]
        with UpsertEncoder[RawEncoded] {
        override final def encode(buf: EncodeBuffer, t: RawEncoded): Unit = {
            t.writeTo(buf)
        }
    }

    implicit def rawQueryEncoder: QueryEncoder[RawEncoded] = RawEncodeSupport
    implicit def rawUpsertEncoder: UpsertEncoder[RawEncoded] = RawEncodeSupport

    def apply(backend: ChannelBackend): RawEncoded = new RawEncoded(backend)

    private class FieldsEncodeSupport(backend: ChannelBackend)
        extends QueryEncoder[Fields] {
        override final def encode(buf: EncodeBuffer, t: Fields): Unit = {
            val raw = RawEncoded(backend)
            raw.writeFields(t)
            raw.writeTo(buf)
        }
    }

    def newFieldsQueryEncoder(backend: ChannelBackend): QueryEncoder[Fields] = new FieldsEncodeSupport(backend)
}

private[cdriver] case class RawField(name: String, decoder: Option[ValueDecoder[_]])

private[cdriver] class RawDecoded {
    var fields: Map[String, Any] = Map.empty
}

private[cdriver] object RawDecoded {
    import CodecUtils._

    private class RawDecodeSupport[NestedEntityType](val needed: Map[String, Option[ValueDecoder[_]]])(implicit val nestedDecodeSupport: QueryResultDecoder[NestedEntityType])
        extends QueryResultDecoder[RawDecoded] {

        override final def decode(buf: DecodeBuffer): RawDecoded = {
            val raw = RawDecoded()
            val len = buf.readInt()
            var what = buf.readByte()
            while (what != Bson.EOO) {
                val name = readNulString(buf)

                needed.get(name) match {
                    case Some(Some(decoder)) =>
                        raw.fields += (name -> decoder.decode(what, buf))
                    case Some(None) =>
                        raw.fields += (name -> readAny[NestedEntityType](what, buf))
                    case None =>
                        skipValue(what, buf)
                }

                what = buf.readByte()
            }

            raw
        }
    }

    def rawQueryResultDecoder[NestedEntityType](needed: Seq[String])(implicit nestedSupport: QueryResultDecoder[NestedEntityType]): QueryResultDecoder[RawDecoded] =
        new RawDecodeSupport[NestedEntityType](Map(needed.map((_, None: Option[ValueDecoder[_]])): _*))

    def rawQueryResultDecoderFields[NestedEntityType](needed: Seq[RawField])(implicit nestedSupport: QueryResultDecoder[NestedEntityType]): QueryResultDecoder[RawDecoded] =
        new RawDecodeSupport[NestedEntityType](Map(needed.map({ f => (f.name, f.decoder) }): _*))

    def apply(): RawDecoded = new RawDecoded()
}
