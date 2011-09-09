package org.beaucatcher.mongo

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._

class CommandResultTest extends TestUtils {
    @Test
    def justOk : Unit = {
        val r = CommandResult(BObject("ok" -> true))
        assertTrue(r.ok)

        val r2 = CommandResult(true)
        assertTrue(r2.ok)

        assertEquals(r.raw, r2.raw)
    }

    @Test
    def intAsBoolean : Unit = {
        val r = CommandResult(BObject("ok" -> 1))
        assertTrue(r.ok)
        val r2 = CommandResult(BObject("ok" -> 0))
        assertFalse(r2.ok)
    }

    @Test
    def throwOnMissingOk : Unit = {
        val e = intercept[BugInSomethingMongoException] {
            CommandResult(BObject()).ok
        }
        assertTrue(e.getMessage.contains("ok"))
    }

    @Test
    def writeResultFields : Unit = {
        val r = WriteResult(ok = true,
            err = Some("this is an error message"),
            n = 10,
            code = Some(5),
            upserted = Some(ObjectId()),
            updatedExisting = Some(false))
        assertEquals(true, r.ok)
        assertEquals(Some("this is an error message"), r.err)
        assertEquals(10, r.n)
        assertEquals(Some(5), r.code)
        assertTrue(r.upserted.isDefined)
        assertEquals(Some(false), r.updatedExisting)
    }
}
