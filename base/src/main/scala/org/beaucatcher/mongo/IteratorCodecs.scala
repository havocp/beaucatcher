package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.wire._

/**
 * Import IteratorCodecs._ to encode and decode `scala.collection.Iterator[(String,Any)]`.
 * The `Any` can be an Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
 * java.util.Date, nested Iterator[(String,Any)], or Seq[Any].
 */
object IteratorCodecs extends IdEncoders with ValueDecoders {
    import CodecUtils._

    private lazy val _anyValueDecoder =
        ValueDecoders.anyValueDecoder[Iterator[(String, Any)]]()

    // can't be implicit since it would always apply
    def anyValueDecoder: ValueDecoder[Any] =
        _anyValueDecoder

    implicit def iteratorQueryEncoder: QueryEncoder[Iterator[(String, Any)]] =
        IteratorUnmodifiedEncoder

    implicit def iteratorQueryResultDecoder: QueryResultDecoder[Iterator[(String, Any)]] =
        IteratorDecoder

    implicit def iteratorUpdateQueryEncoder: UpdateQueryEncoder[Iterator[(String, Any)]] =
        IteratorOnlyIdEncoder

    implicit def iteratorModifierEncoder: ModifierEncoder[Iterator[(String, Any)]] =
        IteratorWithoutIdEncoder

    implicit def iteratorUpsertEncoder: UpsertEncoder[Iterator[(String, Any)]] =
        IteratorUnmodifiedEncoder

    private[beaucatcher] object IteratorUnmodifiedEncoder
        extends QueryEncoder[Iterator[(String, Any)]]
        with UpsertEncoder[Iterator[(String, Any)]] {

        private val fieldWriter: FieldWriter = {
            case (buf, name, doc: Iterator[_]) =>
                writeFieldDocument(buf, name, doc.asInstanceOf[Iterator[(String, Any)]])(IteratorUnmodifiedEncoder)
        }

        override def encode(buf: EncodeBuffer, t: Iterator[(String, Any)]): Unit = {
            val start = writeOpenDocument(buf)

            for (field <- t) {
                writeValueAny(buf, field._1, field._2, fieldWriter)
            }

            writeCloseDocument(buf, start)
        }
    }

    private[beaucatcher] object IteratorWithoutIdEncoder
        extends ModifierEncoder[Iterator[(String, Any)]] {
        override def encode(buf: EncodeBuffer, o: Iterator[(String, Any)]): Unit = {
            IteratorUnmodifiedEncoder.encode(buf, o.filter(_._1 != "_id"))
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object IteratorOnlyIdEncoder
        extends UpdateQueryEncoder[Iterator[(String, Any)]] {
        override def encode(buf: EncodeBuffer, o: Iterator[(String, Any)]): Unit = {
            val idQuery = o.filter(_._1 == "_id")
            if (!idQuery.hasNext)
                throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)")
            IteratorUnmodifiedEncoder.encode(buf, idQuery)
        }
    }

    private[beaucatcher] object IteratorDecoder
        extends QueryResultDecoder[Iterator[(String, Any)]] {
        override def decode(buf: DecodeBuffer): Iterator[(String, Any)] = {
            decodeDocumentIterator(buf, { (what, name, buf) =>
                readAny[Iterator[(String, Any)]](what, buf)
            })
        }
    }
}
