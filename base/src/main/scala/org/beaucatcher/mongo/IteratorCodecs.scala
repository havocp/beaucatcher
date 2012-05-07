package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.wire._

/**
 * Import IteratorCodecs._ to encode and decode `scala.collection.Iterator[(String,Any)]`.
 * The `Any` can be an Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
 * java.util.Date, nested Iterator[(String,Any)], or Seq[Any].
 *
 * Obviously, due to the nature of iterators, you can only encode them one time.
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
        extends IteratorBasedDocumentEncoder[Iterator[(String, Any)]]
        with QueryEncoder[Iterator[(String, Any)]]
        with UpsertEncoder[Iterator[(String, Any)]] {
        override def encodeIterator(t: Iterator[(String, Any)]): Iterator[(String, Any)] = {
            t
        }
    }

    private[beaucatcher] object IteratorWithoutIdEncoder
        extends IteratorBasedDocumentEncoder[Iterator[(String, Any)]]
        with ModifierEncoder[Iterator[(String, Any)]] {
        override def encodeIterator(t: Iterator[(String, Any)]): Iterator[(String, Any)] = {
            t.filter(_._1 != "_id")
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object IteratorOnlyIdEncoder
        extends IteratorBasedDocumentEncoder[Iterator[(String, Any)]]
        with UpdateQueryEncoder[Iterator[(String, Any)]] {
        override def encodeIterator(t: Iterator[(String, Any)]): Iterator[(String, Any)] = {
            val idQuery = t.filter(_._1 == "_id")
            if (!idQuery.hasNext)
                throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)")
            idQuery
        }
    }

    private[beaucatcher] object IteratorDecoder
        extends QueryResultDecoder[Iterator[(String, Any)]] {
        override def decode(buf: DecodeBuffer): Iterator[(String, Any)] = {
            decodeDocumentIterator(buf, { (what, name, buf) =>
                readAny[Iterator[(String, Any)]](what, buf)
            })
        }
        override def decodeIterator(iterator: Iterator[(String, Any)]): Iterator[(String, Any)] = {
            iterator
        }
    }
}
