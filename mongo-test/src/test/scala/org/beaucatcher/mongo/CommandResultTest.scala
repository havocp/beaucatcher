package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._

class CommandResultTest extends TestUtils {
    @Test
    def justOk: Unit = {
        val r = CommandResult(Map("ok" -> true))
        assertTrue(r.ok)

        val r2 = CommandResult(true)
        assertTrue(r2.ok)

        assertEquals(r, r2)
    }

    @Test
    def intAsBoolean: Unit = {
        val r = CommandResult(Map("ok" -> 1))
        assertTrue(r.ok)
        val r2 = CommandResult(Map("ok" -> 0, "err" -> "Did not work"))
        assertFalse(r2.ok)
    }

    @Test
    def throwOnMissingOk: Unit = {
        val e = intercept[BugInSomethingMongoException] {
            CommandResult(Map()).ok
        }
        assertTrue(e.getMessage.contains("ok"))
    }

    @Test
    def writeResultFields: Unit = {
        val r = WriteResult(ok = false,
            err = Some("this is an error message"),
            n = 10,
            code = Some(5),
            upserted = Some(ObjectId()),
            updatedExisting = Some(false))
        assertEquals(false, r.ok)
        assertEquals(Some("this is an error message"), r.err)
        assertEquals(10, r.n)
        assertEquals(Some(5), r.code)
        assertTrue(r.upserted.isDefined)
        assertEquals(Some(false), r.updatedExisting)
    }
}
