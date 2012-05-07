package org.beaucatcher.bson

import java.util.Date

abstract trait TestUtils {
    protected def intercept[E <: Throwable: Manifest](block: => Unit): E = {
        val expectedClass = manifest.erasure.asInstanceOf[Class[E]]
        var thrown: Option[Throwable] = None
        try {
            block
        } catch {
            case t: Throwable => thrown = Some(t)
        }
        thrown match {
            case Some(t) if expectedClass.isAssignableFrom(t.getClass) =>
                t.asInstanceOf[E]
            case Some(t) =>
                throw new Exception("Expected exception %s was not thrown, got %s".format(expectedClass.getName, t), t)
            case None =>
                throw new Exception("Expected exception %s was not thrown".format(expectedClass.getName))
        }
    }

    protected def describeFailure[A](desc: String)(code: => A): A = {
        try {
            code
        } catch {
            case t: Throwable =>
                println("Failure on: '%s'".format(desc))
                throw t
        }
    }

    final def anyToIterators(x: Any): Any = {
        x match {
            case null =>
                null
            case child: Map[_, _] =>
                mapValuesToIterators(child.asInstanceOf[Map[String, Any]]).iterator
            case s: Seq[_] =>
                s.map(anyToIterators(_))
            case v =>
                v
        }
    }

    final def mapValuesToIterators(m: Map[String, Any]): Map[String, Any] = {
        m.mapValues(anyToIterators(_))
    }

    final def anyToMaps(x: Any): Any = {
        x match {
            case null =>
                null
            case child: Iterator[_] =>
                iteratorValuesToMaps(child.asInstanceOf[Iterator[(String, Any)]]).toMap[String, Any]
            case s: Seq[_] =>
                s.map(anyToMaps(_))
            case v =>
                v
        }
    }

    final def iteratorValuesToMaps(i: Iterator[(String, Any)]): Iterator[(String, Any)] = {
        i.map({ kv =>
            (kv._1, anyToMaps(kv._2))
        })
    }

    private val someJavaDate = new Date(837017400000L)

    final def makeMapManyTypes() = {
        Map("null" -> null,
            "int" -> 42,
            "long" -> 37L,
            "double" -> 3.14159,
            "boolean" -> true,
            "string" -> "quick brown fox",
            "date" -> someJavaDate,
            "timestamp" -> Timestamp((someJavaDate.getTime / 1000).toInt, 1),
            "objectid" -> ObjectId("4dbf8ea93364e3bd9745723c"),
            "binary" -> Binary(new Array[Byte](10), BsonSubtype.GENERAL),
            "map_int" -> Map[String, Int]("a" -> 20, "b" -> 21),
            "map_date" -> Map[String, Date]("a" -> someJavaDate, "b" -> someJavaDate),
            "seq_string" -> List("a", "b", "c", "d"),
            "seq_int" -> List(1, 2, 3, 4),
            "obj" -> Map[String, Any]("foo" -> 6789, "bar" -> 4321))
    }

    final def makeSeqManyTypes() = {
        // a non-homogeneous-typed array is pretty much nonsense, but JavaScript
        // lets you do whatever, so we let you do whatever.
        Seq[Any](null,
            42,
            37L,
            3.14159,
            true,
            "quick brown fox",
            someJavaDate,
            Timestamp((someJavaDate.getTime / 1000).toInt, 1),
            ObjectId("4dbf8ea93364e3bd9745723c"),
            Binary(new Array[Byte](10), BsonSubtype.GENERAL),
            Map[String, Int]("a" -> 20, "b" -> 21),
            Map[String, Date]("a" -> someJavaDate, "b" -> someJavaDate),
            List("a", "b", "c", "d"),
            List(1, 2, 3, 4),
            Map[String, Any]("foo" -> 6789, "bar" -> 4321))
    }
}
