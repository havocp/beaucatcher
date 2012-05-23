package org.beaucatcher.mongo.jdriver

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._
import foo._

class CollectionTest
    extends AbstractCollectionTest
    with JavaDriverTestContextProvider {

    @Test
    def usingExpectedDriver(): Unit = {
        assertTrue("expecting to use Java driver",
            implicitly[Context].driver.getClass.getSimpleName.contains("JavaDriver"))
    }
}
