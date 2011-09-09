package org.beaucatcher.bson

import org.junit.Assert._
import org.junit._

import org.bson.{ types => j }

class ObjectIdTest extends TestUtils {

    @org.junit.Before
    def setup() {
    }

    @Test
    def constructFromString() : Unit = {
        val oId = ObjectId("4dbf8ea93364e3bd9745723c")

        assertEquals("4dbf8ea93364e3bd9745723c", oId.toString)
    }

    @Test
    def constructFromParts() : Unit = {
        val oId = ObjectId(123456, 876543, 400701)
        assertEquals(123456, oId.time)
        assertEquals(876543, oId.machine)
        assertEquals(400701, oId.inc)
    }

    @Test
    def timeMillis() : Unit = {
        val oId = ObjectId("4dbf8ea93364e3bd9745723c")
        assertEquals(oId.time * 1000L, oId.timeMillis)
    }

    @Test
    def roundTripThroughParts() : Unit = {
        // one fixed case
        val oId = ObjectId("4dbf8ea93364e3bd9745723c")
        val fromParts = ObjectId(oId.time, oId.machine, oId.inc)
        assertEquals(oId, fromParts)
        assertEquals(oId.toString, fromParts.toString)

        // generate various cases that might trigger signed/unsigned or endian-related
        // problems with the hex encoding, etc.
        val numbers = Seq(
            0xff000000, 0x00ff0000, 0x0000ff00, 0x000000ff,
            0x81000000, 0x00810000, 0x00008100, 0x00000081)
        for {
            t <- numbers
            m <- numbers
            i <- numbers
        } {
            val id = ObjectId(t, m, i)
            assertEquals(t, id.time)
            assertEquals(m, id.machine)
            assertEquals(i, id.inc)
        }
    }

    @Test
    def notCaseSensitive() : Unit = {
        val lower = "4dbf8ea93364e3bd9745723c"
        val upper = lower.toUpperCase()
        assertTrue(lower != upper)
        assertEquals(lower, upper.toLowerCase())

        val oId = ObjectId(lower)
        val fromUpperCase = ObjectId(upper)

        assertEquals(oId, fromUpperCase)
        assertEquals(oId.time, fromUpperCase.time)
        assertEquals(oId.machine, fromUpperCase.machine)
        assertEquals(oId.inc, fromUpperCase.inc)

        // the upper case should get canonicalized by toString
        // (i.e. we should never generate upper case)
        assertEquals(lower, oId.toString)
        assertEquals(lower, fromUpperCase.toString)
    }

    @Test
    def equalsAndHashCode() : Unit = {
        val oId = ObjectId("4dbf8ea93364e3bd9745723c")
        val sameId = ObjectId("4dbf8ea93364e3bd9745723c")
        val differentId = ObjectId("9ebf8ea63366e3bd9743723f")

        assertEquals(oId, sameId)
        assertEquals(sameId, oId)
        assertFalse(oId == differentId)
        assertFalse(differentId == oId)
        assertEquals(oId, oId)

        assertEquals(oId.hashCode, sameId.hashCode)
        assertEquals(oId.time, sameId.time)
        assertEquals(oId.machine, sameId.machine)
        assertEquals(oId.inc, sameId.inc)
    }

    @Test
    def generatingObjectId : Unit = {
        val one = ObjectId()
        val two = ObjectId()
        val three = ObjectId()

        assertEquals(one.machine, two.machine)
        assertEquals(one.machine, three.machine)

        assertFalse(one.inc == two.inc)
        assertFalse(two.inc == three.inc)
        // not reliable since sbt can run tests in parallel
        //assertEquals(one.inc + 1, two.inc)
        //assertEquals(one.inc + 2, three.inc)
    }
}
