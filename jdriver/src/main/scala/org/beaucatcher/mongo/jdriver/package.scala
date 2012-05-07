package org.beaucatcher.mongo

import org.bson.{ types => j }
import org.bson.BSONObject
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, MongoException => JavaMongoException, _ }

package object jdriver {
    private[jdriver] implicit def asJavaObjectId(o : ObjectId) = new j.ObjectId(o.time, o.machine, o.inc)

    private[jdriver] implicit def asScalaObjectId(o : j.ObjectId) = ObjectId(o.getTimeSecond(), o.getMachine(), o.getInc())

    private[jdriver] implicit def asJavaTimestamp(t : Timestamp) = new j.BSONTimestamp(t.time, t.inc)

    private[jdriver] implicit def asScalaTimestamp(t : j.BSONTimestamp) = Timestamp(t.getTime(), t.getInc())

    private[jdriver] implicit def asJavaBinary(b : Binary) = new j.Binary(BsonSubtype.toByte(b.subtype), b.data)

    private[jdriver] implicit def asScalaBinary(b : j.Binary) = Binary(b.getData(), BsonSubtype.fromByte(b.getType()).get)

    private[jdriver] class EnrichedContext(context : DriverContext) {
        def asJavaContext() = {
            context match {
                case null =>
                    throw new BugInSomethingMongoException("null mongo.Context")
                case j : JavaDriverContext =>
                    j
                case wrong =>
                    throw new BugInSomethingMongoException("mongo.Context passed to jdriver is not from jdriver; context type is " + wrong.getClass.getSimpleName)
            }
        }
    }

    private[jdriver] implicit def context2enriched(context : DriverContext) = new EnrichedContext(context)

    private[jdriver] implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
        WriteResult({ toIterator(j.getLastError()) })
    }

    private[jdriver] implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
        CommandResult({ toIterator(j) })
    }

    private[jdriver] def toIterator(obj : BSONObject) : Iterator[(String, Any)] = {
        val keyIter = obj.keySet().iterator()
        new Iterator[(String, Any)]() {
            override def hasNext = keyIter.hasNext()
            override def next() = {
                val key = keyIter.next()
                (key -> fromJava(obj.get(key)))
            }
        }
    }

    private[jdriver] def fromIterator(i : Iterator[(String, Any)]) : DBObject = {
        val o = new BasicDBObject()
        for (kv <- i) {
            o.put(kv._1, toJava(kv._2))
        }
        o
    }

    private[jdriver] def fromSeq(s : Seq[Any]) : BasicDBList = {
        val l = new BasicDBList()
        for (e <- s) {
            l.add(toJava(e))
        }
        l
    }

    private[jdriver] def fromJava(x : Any) : Any = {
        import scala.collection.JavaConverters._

        x match {
            // null first because it otherwise matches all the instanceofs
            case null =>
                null
            case b : j.Binary =>
                b : Binary
            case t : j.BSONTimestamp =>
                t : Timestamp
            case oid : j.ObjectId =>
                oid : ObjectId
            case m : java.util.Map[_, _] =>
                m.asScala.toMap.asInstanceOf[Map[String, Any]].mapValues(fromJava(_)).iterator : Iterator[(String, Any)]
            case l : java.util.List[_] =>
                l.asScala.map(fromJava(_)) : Seq[Any]
            // Note: BSONObject that also implement List should be caught by the
            // case for List above, so we should get a Seq for them.
            case bsonObj : BSONObject =>
                toIterator(bsonObj)
            case a : Array[Byte] =>
                // Sometimes Casbah gives us a raw byte array rather than
                // a Binary object, apparently?
                Binary(a)
            case x =>
                // Fall back to handle Scala types (including all the plain integers and so on)
                x
        }
    }

    private[jdriver] def toJava(x : Any) : AnyRef = {
        import scala.collection.JavaConverters._
        x match {
            case null =>
                null
            case b : Binary =>
                b : j.Binary
            case t : Timestamp =>
                t : j.BSONTimestamp
            case oid : ObjectId =>
                oid : j.ObjectId
            case v : Seq[_] =>
                v.map({ e => toJava(e) }).asJava : java.util.List[_]
            case v : Iterator[_] =>
                val i = v.asInstanceOf[Iterator[(String, Any)]]
                fromIterator(i) : DBObject
            case v =>
                // handles strings and ints and stuff
                v.asInstanceOf[AnyRef]
        }
    }

    private[jdriver] def convertDocumentToJava[D](doc : D)(implicit encodeSupport : DocumentEncoder[D]) : DBObject = {
        fromIterator(encodeSupport.encodeIterator(doc))
    }

    private[jdriver] def convertQueryToJava[Q](query : Q)(implicit querySupport : QueryEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertModifierToJava[Q](query : Q)(implicit querySupport : ModifierEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertUpsertToJava[Q](query : Q)(implicit querySupport : UpsertEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertUpdateQueryToJava[Q](query : Q)(implicit querySupport : UpdateQueryEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertResultFromJava[E](obj : BSONObject)(implicit entitySupport : QueryResultDecoder[E]) : E = {
        entitySupport.decodeIterator(toIterator(obj))
    }

    private[jdriver] def convertValueFromJava[V](v : AnyRef)(implicit valueDecoder : ValueDecoder[V]) : V = {
        valueDecoder.decodeAny(fromJava(v))
    }

    private[jdriver] def putIdToJava[I](obj : BSONObject, name : String, id : I)(implicit idEncoder : IdEncoder[I]) : Unit = {
        obj.put(name, toJava(idEncoder.encodeFieldAny(id)))
    }
}
