package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, _ }

package object casbah {
    import j.JavaConversions._

    object Implicits {
        private[casbah] implicit def asScalaBObject(bsonObj : BSONObject) = {
            import scala.collection.JavaConversions._

            val keys = bsonObj.keySet().iterator()
            val fields = for { key <- keys }
                yield (key, wrapJavaAsBValue(bsonObj.get(key)))
            BObject(fields.toList)
        }

        private[casbah] implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
            new WriteResult({ asScalaBObject(j.getLastError()) })
        }

        private[casbah] implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
            new CommandResult({ asScalaBObject(j) })
        }
    }

    /* Mutable BSONObject/DBObject implementation used to save to MongoDB API */
    private[casbah] class BObjectBSONObject extends BSONObject {
        import scala.collection.JavaConversions._

        private[this] var bvalue : BObject = BObject.empty

        def this(b : BObject) = {
            this()
            bvalue = b
        }

        override def toString = "BObjectBSONObject(%s)".format(bvalue)

        /* BSONObject interface */
        override def containsField(s : String) : Boolean = {
            bvalue.contains(s)
        }
        override def containsKey(s : String) : Boolean = containsField(s)

        override def get(key : String) : AnyRef = {
            bvalue.get(key) match {
                case Some(bvalue) =>
                    bvalue.unwrappedAsJava
                case None =>
                    null
            }
        }

        override def keySet() : java.util.Set[String] = {
            bvalue.keySet
        }

        // returns previous value
        override def put(key : String, v : AnyRef) : AnyRef = {
            val previous = get(key)
            bvalue = bvalue + (key -> wrapJavaAsBValue(v))
            previous
        }

        override def putAll(bsonObj : BSONObject) : Unit = {
            for { key <- bsonObj.keySet() }
                put(key, wrapJavaAsBValue(bsonObj.get(key)))
        }

        override def putAll(m : java.util.Map[_, _]) : Unit = {
            for { key <- m.keySet() }
                put(key.asInstanceOf[String], wrapJavaAsBValue(m.get(key)))
        }

        override def removeField(key : String) : AnyRef = {
            val previous = get(key)
            bvalue = bvalue - key
            previous
        }

        override def toMap() : java.util.Map[_, _] = {
            bvalue.unwrappedAsJava
        }
    }

    /**
     * adds DBObject extensions to BSONObject.
     * This is an internal implementation class not exported by the library.
     */
    private[casbah] class BObjectDBObject(b : BObject) extends BObjectBSONObject(b) with DBObject {
        private[this] var isPartial : Boolean = false

        def this() = {
            this(BObject.empty)
        }

        override def isPartialObject() : Boolean = isPartial

        override def markAsPartialObject() : Unit = {
            isPartial = true
        }

        override def toString = "BObjectDBObject(%s)".format(b)
    }
}
