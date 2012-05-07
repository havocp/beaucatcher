package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.wire._

/**
 * Import MapCodecs._ to encode and decode `scala.collection.immutable.Map[String,Any]`.
 * The `Any` can be an Int, Long, Double, String, Null, Boolean, ObjectId, Binary, Timestamp,
 * java.util.Date, nested Map[String,Any], or Seq[Any].
 */
object MapCodecs extends IdEncoders with ValueDecoders {
    import CodecUtils._

    private lazy val _anyValueDecoder =
        ValueDecoders.anyValueDecoder[Map[String, Any]]()

    // can't be implicit since it would always apply
    def anyValueDecoder: ValueDecoder[Any] =
        _anyValueDecoder

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
        extends IteratorBasedDocumentEncoder[Map[String, Any]]
        with QueryEncoder[Map[String, Any]]
        with UpsertEncoder[Map[String, Any]] {
        override def encodeIterator(t: Map[String, Any]): Iterator[(String, Any)] = {
            mapToIterator(t)
        }
    }

    private[beaucatcher] object MapWithoutIdEncoder
        extends IteratorBasedDocumentEncoder[Map[String, Any]]
        with ModifierEncoder[Map[String, Any]] {
        override def encodeIterator(t: Map[String, Any]): Iterator[(String, Any)] = {
            mapToIterator(t.filterKeys(_ != "_id"))
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object MapOnlyIdEncoder
        extends IteratorBasedDocumentEncoder[Map[String, Any]]
        with UpdateQueryEncoder[Map[String, Any]] {
        override def encodeIterator(t: Map[String, Any]): Iterator[(String, Any)] = {
            val id = t.getOrElse("_id", throw new BugInSomethingMongoException("only objects with an _id field work here (you need an _id to save() for example)"))
            Iterator("_id" -> id)
        }
    }

    private[beaucatcher] object MapDecoder
        extends QueryResultDecoder[Map[String, Any]] {
        override def decode(buf: DecodeBuffer): Map[String, Any] = {
            val b = Map.newBuilder[String, Any]
            decodeDocumentForeach(buf, { (what, name, buf) =>
                b += (name -> readAny[Map[String, Any]](what, buf))
            })
            b.result()
        }

        override def decodeIterator(iterator: Iterator[(String, Any)]): Map[String, Any] = {
            iteratorToMap(iterator)
        }
    }

    private def decodeAny(value: Any): Any = {
        value match {
            case i: Iterator[_] =>
                iteratorToMap(i.asInstanceOf[Iterator[(String, Any)]])
            case x =>
                x
        }
    }

    private[beaucatcher] def iteratorToMap(iterator: Iterator[(String, Any)]): Map[String, Any] = {
        val b = Map.newBuilder[String, Any]
        for (pair <- iterator) {
            b += (pair._1 -> decodeAny(pair._2))
        }
        b.result()
    }

    private[beaucatcher] def mapToIterator(m: Map[String, Any]): Iterator[(String, Any)] = {
        m.mapValues({ v =>
            v match {
                case null =>
                    null
                case child: Map[_, _] =>
                    mapToIterator(child.asInstanceOf[Map[String, Any]])
                case x =>
                    x
            }
        }).iterator
    }
}
