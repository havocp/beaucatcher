package org.beaucatcher.jdriver

import java.text.SimpleDateFormat
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.jdriver.Implicits._
import com.mongodb.BasicDBObject
import java.util.Date
import org.junit.Assert._
import org.junit._

class BsonJavaDriverTest {

    @org.junit.Before
    def setup() {
    }

    protected def makeObjectWithImplicits() = {
        val obj = new BasicDBObject()
        obj.put("a", 30)
        obj.put("b", "foo")
        BObject("int" -> 42,
            "bsonobj" -> obj)
    }

    protected def makeArrayWithImplicits() = {
        val obj = new BasicDBObject()
        obj.put("a", 30)
        obj.put("b", "foo")
        // a non-homogeneous-typed array is pretty much nonsense, but JavaScript
        // lets you do whatever, so we let you do whatever.
        BArray(42, obj)
    }

    @Test
    def buildBObject() = {
        // Test that we can use implicits on a DBObject to build a BObject
        // We're mostly testing that this compiles rather than that it runs...

        val bobj = makeObjectWithImplicits()
        val fromDBObj = bobj.getOrElse("bsonobj", throw new Exception("bsonobj not got"))
        fromDBObj match {
            case o: BObject =>
                assertTrue(o.contains("a"))
                assertTrue(o.contains("b"))
                assertTrue(!o.contains("c"))
            case _ =>
                throw new Exception("DBObject implicitly converted to wrong type in object")
        }
    }

    @Test
    def buildBArray() = {
        // Test that we can use implicits on a DBObject to build a BArray
        // We're mostly testing that this compiles rather than that it runs...

        val barray = makeArrayWithImplicits()
        val fromDBObj = barray(1)
        fromDBObj match {
            case o: BObject =>
                assertTrue(o.contains("a"))
                assertTrue(o.contains("b"))
                assertTrue(!o.contains("c"))
            case _ =>
                throw new Exception("DBObject implicitly converted to wrong type in array")
        }
    }
}
