package org.beaucatcher.bson

import org.bson.{ types => j }
import org.bson.BSONObject
import JavaConversions._

/* Mutable BSONObject/DBObject implementation used to save to MongoDB API */
private[beaucatcher] trait BValueBSONObject[Repr <: BValue]
    extends BSONObject {
    import scala.collection.JavaConverters._

    protected[this] var bvalue : Repr

    override def toString = getClass.getSimpleName + "(%s)".format(bvalue)

    override def containsKey(s : String) : Boolean = containsField(s)

    protected def unwrap(v : BValue) = {
        v match {
            case o : BObject =>
                new BObjectBSONObject(o)
            case a : BArray =>
                new BArrayBSONObject(a)
            case _ =>
                v.unwrappedAsJava
        }
    }

    override def putAll(bsonObj : BSONObject) : Unit = {
        for { key <- bsonObj.keySet().asScala }
            put(key, wrapJavaAsBValue(bsonObj.get(key)))
    }

    override def putAll(m : java.util.Map[_, _]) : Unit = {
        for { key <- m.keySet().asScala }
            put(key.asInstanceOf[String], wrapJavaAsBValue(m.get(key)))
    }
}

private[beaucatcher] class BObjectBSONObject(override var bvalue : BObject = BObject.empty)
    extends BValueBSONObject[BObject] {
    import scala.collection.JavaConverters._

    /* BSONObject interface */
    override def containsField(s : String) : Boolean = {
        bvalue.contains(s)
    }

    override def get(key : String) : AnyRef = {
        bvalue.get(key) match {
            case Some(bvalue) =>
                unwrap(bvalue)
            case None =>
                null
        }
    }

    override def keySet() : java.util.Set[String] = {
        bvalue.asJava.keySet
    }

    // returns previous value
    override def put(key : String, v : AnyRef) : AnyRef = {
        val previous = get(key)
        bvalue = bvalue + (key -> wrapJavaAsBValue(v))
        previous
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

// We leave the mutators on AbstractList throwing an exception as they do by default,
// because while Casbah will do things like add an _id field to an object, it
// doesn't mess with arrays as far as I know
private[beaucatcher] class BArrayBSONObject(override var bvalue : BArray = BArray.empty)
    extends java.util.AbstractList[AnyRef] with BValueBSONObject[BArray] {
    import scala.collection.JavaConverters._

    private def asInt(key : String) : Option[Int] = try {
        Some(Integer.parseInt(key))
    } catch {
        case e : NumberFormatException =>
            None
    }

    private def withIntOrElse[A](key : String)(body : Int => A)(fallback : => A) : A = {
        val i = asInt(key)
        if (i.isDefined)
            (i map body).get
        else
            fallback
    }

    /* BSONObject interface */
    override def containsField(s : String) : Boolean = {
        withIntOrElse(s) { i =>
            i >= 0 && i < bvalue.size
        } {
            false
        }
    }

    override def get(key : String) : AnyRef = {
        withIntOrElse(key) { i =>
            try {
                get(i)
            } catch {
                case e : IndexOutOfBoundsException =>
                    null
            }
        } {
            null
        }
    }

    override def keySet() : java.util.Set[String] = {
        (for (i <- 0 to (bvalue.size - 1))
            yield i.toString).toSet.asJava
    }

    // returns previous value
    override def put(key : String, v : AnyRef) : AnyRef = {
        val previous = get(key)
        withIntOrElse(key) { i =>
            val missing = i - bvalue.size
            val more : Seq[BValue] = if (missing > 0) {
                for (j <- 0 to missing)
                    yield BNull
            } else {
                Seq.empty
            }
            bvalue = bvalue ++ BArray(more)
            assert(bvalue.size > i)
            assert(missing <= 0 || bvalue.size == (i + 1))
            bvalue = bvalue.updated(i, wrapJavaAsBValue(v))
        } {
        }
        previous
    }

    override def removeField(key : String) : AnyRef = {
        val previous = get(key)
        withIntOrElse(key) { i =>
            if (i == (bvalue.size - 1)) {
                bvalue = bvalue.dropRight(1)
            } else {
                bvalue = bvalue.updated(i, BNull)
            }
        } {

        }
        previous
    }

    override def toMap() : java.util.Map[_, _] = {
        println("toMap")
        BObject(bvalue.zipWithIndex.toList map { kv => (kv._2.toString, kv._1) }).unwrappedAsJava
    }

    // AbstractList interface

    override def get(i : Int) = {
        unwrap(bvalue(i))
    }

    override def size : Int = {
        bvalue.size
    }
}
