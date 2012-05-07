package org.beaucatcher.driver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._

class DriverPackageTest extends TestUtils {

    @Test
    def testIndexNameComputation() {
        import MapCodecs._

        val answers = Map(
            Map("a" -> 1) -> "a_1",
            Map("b" -> -1) -> "b_-1",
            Map("c" -> 1, "d" -> -1) -> "c_1_d_-1")
        for (kv <- answers) {
            assertEquals(kv._2, defaultIndexName(kv._1))
        }
    }
}
