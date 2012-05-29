package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo.cdriver._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._

class CollectionTest
    extends AbstractCollectionTest
    with ChannelDriverTestContextProvider {

    @Test
    def usingExpectedDriver(): Unit = {
        assertTrue("expecting to use channel driver",
            implicitly[Context].driver.getClass.getSimpleName.contains("ChannelDriver"))
    }
}
