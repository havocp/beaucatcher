package org.beaucatcher.mongo.jdriver

import org.bson.{ types => j }
import org.beaucatcher.bson._
import org.junit.Assert._
import org.junit.Before
import org.junit.Test

class JavaConversionsTest extends TestUtils {

    @org.junit.Before
    def setup() {
    }

    @Test
    def convertObjectId() : Unit = {
        val jId = new j.ObjectId()
        val sId = ObjectId()
        val fromS : j.ObjectId = sId
        val fromJ : ObjectId = jId

        assertEquals(jId.toString, fromJ.toString)
        assertEquals(sId.toString, fromS.toString)
    }

    @Test
    def convertTimestamp() : Unit = {
        val jT = new j.BSONTimestamp(12345, 6)
        val sT = Timestamp(54321, 7)
        val fromS : j.BSONTimestamp = sT
        val fromJ : Timestamp = jT

        assertEquals(sT.time, fromS.getTime())
        assertEquals(sT.inc, fromS.getInc())

        assertEquals(jT.getTime(), fromJ.time)
        assertEquals(jT.getInc(), fromJ.inc)
    }

    @Test
    def convertBinary() : Unit = {
        val bytes1 = new Array[Byte](10)
        for (i <- 0 to 9)
            bytes1.update(i, i.toByte)
        val bytes2 = new Array[Byte](10)
        for (i <- 9 to 0)
            bytes2.update(i, i.toByte)

        val jB = new j.Binary(org.bson.BSON.B_GENERAL, bytes1)
        val sB = Binary(bytes2, BsonSubtype.GENERAL)
        val fromS : j.Binary = sB
        val fromJ : Binary = jB

        assertTrue(sB.data sameElements fromS.getData())
        assertEquals(BsonSubtype.toByte(sB.subtype), fromS.getType())

        assertTrue(jB.getData() sameElements fromJ.data)
        assertEquals(jB.getType(), BsonSubtype.toByte(fromJ.subtype))

        // for efficiency we don't copy the byte[], but
        // this isn't really in the API contract nor is it
        // all that safe in theory; in practice, it
        // seems like an app would have to deliberately
        // shoot itself in the foot to make this break.
        // there's no way to do immutable byte arrays right?
        assertEquals(sB.data, fromS.getData())
        assertEquals(jB.getData(), fromJ.data)
    }

    private def assertJavaValue(j : AnyRef) : Unit = {
        if (j != null) {
            val klassName = j.getClass.getName
            //System.out.println(k + "=" + klassName)
            assertFalse("found a scala type in supposed Java value " + klassName,
                (klassName.startsWith("scala.") && !klassName.endsWith("Wrapper")))
            assertTrue("found unexpected type in supposed Java value " + klassName,
                klassName.contains("java") ||
                    klassName.startsWith("com.mongodb.") ||
                    (klassName.startsWith("scala.collection.JavaConversions") &&
                        klassName.endsWith("Wrapper")) ||
                        klassName.startsWith("org.bson.types."))

            // recurse
            j match {
                case c : java.util.Collection[_] =>
                    val i = c.iterator()
                    while (i.hasNext())
                        assertJavaValue(i.next().asInstanceOf[AnyRef])
                case _ =>
            }
        }
    }

    private def testJavaRoundTrip(v : Any) : Unit = {
        try {
            val j = toJava(anyToIterators(v))
            assertJavaValue(j)
            val back = anyToMaps(fromJava(j))
            assertEquals(v, back)
        } catch {
            case e =>
                System.err.println("Failure on: " + v)
                throw e
        }
    }

    // check that wrap/unwrap from/to Java behaves sensibly
    @Test
    def javaUnwrapWorks() = {
        val obj = makeMapManyTypes()
        for ((k, v) <- obj) {
            testJavaRoundTrip(v)
        }
        testJavaRoundTrip(obj)

        val a = makeSeqManyTypes()
        testJavaRoundTrip(a)
    }
}
