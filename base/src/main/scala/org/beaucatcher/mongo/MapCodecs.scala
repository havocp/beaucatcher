package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.wire._

/**
 * Import MapCodecs._ to encode and decode `scala.immutable.Map[String,Any]`.
 * The `Any` can be an Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
 * java.util.Date, nested Map[String,Any], or Seq[Any].
 */
object MapCodecs extends IdEncoders with ValueDecoders {
    import CodecUtils._

    implicit def mapQueryEncoder: QueryEncoder[Map[String, Any]] =
        MapUnmodifiedEncoder

    implicit def mapQueryResultDecoder: QueryResultDecoder[Map[String, Any]] =
        MapDecoder

    implicit def mapUpdateQueryEncoder: UpdateQueryEncoder[Map[String, Any]] =
        MapOnlyIdEncoder

    implicit def mapModifierEncoder: ModifierEncoder[Map[String, Any]] =
        MapWithoutIdEncoder

    implicit def mapUpsertEncoder: UpsertEncoder[Map[String, Any]] =
        MapUnmodifiedEncoder

    private[beaucatcher] object MapUnmodifiedEncoder
        extends QueryEncoder[Map[String, Any]]
        with UpsertEncoder[Map[String, Any]] {
        override def encode(buf: EncodeBuffer, t: Map[String, Any]): Unit = {
            val start = writeOpenDocument(buf)

            for (field <- t) {
                writeValueAny(buf, field._1, field._2)
            }

            writeCloseDocument(buf, start)
        }
    }

    private[beaucatcher] object MapWithoutIdEncoder
        extends ModifierEncoder[Map[String, Any]] {
        override def encode(buf: EncodeBuffer, o: Map[String, Any]): Unit = {
            MapUnmodifiedEncoder.encode(buf, o - "_id")
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object MapOnlyIdEncoder
        extends UpdateQueryEncoder[Map[String, Any]] {
        override def encode(buf: EncodeBuffer, o: Map[String, Any]): Unit = {
            val idQuery = Map("_id" -> o.getOrElse("_id", throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)")))
            MapUnmodifiedEncoder.encode(buf, idQuery)
        }
    }

    private[beaucatcher] object MapDecoder
        extends QueryResultDecoder[Map[String, Any]] {
        override def decode(buf: DecodeBuffer): Map[String, Any] = {
            val len = buf.readInt()
            if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
                buf.skipBytes(len - 4)
                Map.empty
            } else {
                val b = Map.newBuilder[String, Any]

                var what = buf.readByte()
                while (what != Bson.EOO) {

                    val name = readNulString(buf)

                    b += (name -> readAny[Map[String, Any]](what, buf))

                    what = buf.readByte()
                }

                b.result()
            }
        }
    }

    private def readArray(buf: DecodeBuffer): IndexedSeq[Any] = {
        val len = buf.readInt()
        if (len == Bson.EMPTY_DOCUMENT_LENGTH) {
            buf.skipBytes(len - 4)
            IndexedSeq.empty[Any]
        } else {
            val b = IndexedSeq.newBuilder[Any]

            var what = buf.readByte()
            while (what != Bson.EOO) {

                // the names in an array are just the indices, so nobody cares
                skipNulString(buf)

                b += readAny[Map[String, Any]](what, buf)

                what = buf.readByte()
            }

            b.result()
        }
    }
}
