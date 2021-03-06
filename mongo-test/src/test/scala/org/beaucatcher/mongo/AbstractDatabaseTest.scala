package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._

abstract class AbstractDatabaseTest
    extends TestUtils {
    self: ContextProvider =>

    // has to be lazy to get right initialization order
    lazy val db = mongoContext.database

    @org.junit.Before
    def setup() {
        db.sync.dropDatabase()
    }

    @Test
    def createAndListCollections() = {
        assertEquals(Nil, db.sync.collectionNames.toList)
        db.sync.createCollection("created1", CreateCollectionOptions(capped = Some(true), size = Some(1024 * 10)))
        db.sync.createCollection("created2", CreateCollectionOptions(capped = Some(true), size = Some(1024 * 10)))
        assertEquals(List(db.name + ".created1", db.name + ".created2"), db.sync.collectionNames.toList)
        db.sync.dropDatabase()
        assertEquals(Nil, db.sync.collectionNames.toList)
    }
}
