import java.text.SimpleDateFormat
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.casbah.Implicits._
import com.mongodb.casbah.commons.MongoDBObject
import org.bson.types.Binary
import org.bson.types.ObjectId
import org.bson.types.BSONTimestamp
import java.util.Date
import org.joda.time.{ DateTimeZone, DateTime }
import org.junit.Assert._
import org.junit._

class BsonCasbahTest {

    @org.junit.Before
    def setup() {
    }

    protected def makeObjectWithImplicits() = {
        BObject("int" -> 42,
            "bsonobj" -> MongoDBObject("a" -> 30, "b" -> "foo"))
    }

    protected def makeArrayWithImplicits() = {
        // a non-homogeneous-typed array is pretty much nonsense, but JavaScript
        // lets you do whatever, so we let you do whatever.
        BArray(42,
            MongoDBObject("a" -> 30, "b" -> "foo"))
    }

    @Test
    def buildBObject() = {
        // Test that we can use implicits on a DBObject to build a BObject
        // We're mostly testing that this compiles rather than that it runs...

        val bobj = makeObjectWithImplicits()
        val fromDBObj = bobj.getOrElse("bsonobj", throw new Exception("bsonobj not got"))
        fromDBObj match {
            case o : BObject =>
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
            case o : BObject =>
                assertTrue(o.contains("a"))
                assertTrue(o.contains("b"))
                assertTrue(!o.contains("c"))
            case _ =>
                throw new Exception("DBObject implicitly converted to wrong type in array")
        }
    }
}
