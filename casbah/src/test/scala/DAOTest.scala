import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.casbah._
import org.beaucatcher.mongo._
import org.bson.types._
import org.junit.Assert._
import org.junit._

package foo {
    case class Foo(_id : ObjectId, intField : Int, stringField : String)

    object Foo extends CollectionOperations[Foo, ObjectId]
        with CasbahTestProvider {
        override val collectionName = "foo"

        def customQuery[E](implicit chooser : SyncDAOChooser[E]) = {
            syncDAO[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithIntId(_id : Int, intField : Int, stringField : String)

    object FooWithIntId extends CollectionOperations[FooWithIntId, Int]
        with CasbahTestProvider {
        override val collectionName = "fooWithIntId"

        def customQuery[E](implicit chooser : SyncDAOChooser[E]) = {
            syncDAO[E].find(BObject("intField" -> 23))
        }
    }
}

class DAOTest {
    import foo._

    @org.junit.Before
    def setup() {
        Foo.bobjectSyncDAO.remove(BObject())
        FooWithIntId.bobjectSyncDAO.remove(BObject())
    }

    @Test
    def testSaveAndFindOneCaseClass() {
        val foo = Foo(new ObjectId(), 23, "woohoo")
        Foo.caseClassSyncDAO.save(foo)
        val maybeFound = Foo.caseClassSyncDAO.findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testSaveAndFindOneCaseClassWithIntId() {
        val foo = FooWithIntId(89, 23, "woohoo")
        FooWithIntId.caseClassSyncDAO.save(foo)
        val maybeFound = FooWithIntId.caseClassSyncDAO.findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testCustomQueryReturnsVariousEntityTypes() {
        val foo = Foo(new ObjectId(), 23, "woohoo")
        Foo.caseClassSyncDAO.save(foo)

        val objects = Foo.customQuery[BObject].toIndexedSeq
        assertEquals(1, objects.size)
        assertEquals(BInt32(23), objects(0).get("intField").get)
        assertEquals(BString("woohoo"), objects(0).get("stringField").get)

        val caseClasses = Foo.customQuery[Foo].toIndexedSeq
        assertEquals(1, caseClasses.size)
        val f = caseClasses(0)
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)
    }

    @Test
    def testFindByIDAllResultTypes() {
        val foo = Foo(new ObjectId(), 23, "woohoo")
        Foo.caseClassSyncDAO.save(foo)

        val o = Foo.syncDAO[BObject].findOneById(foo._id).get
        assertEquals(BInt32(23), o.get("intField").get)
        assertEquals(BString("woohoo"), o.get("stringField").get)

        val f = Foo.syncDAO[Foo].findOneById(foo._id).get
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)

        // should not compile because FooWithIntId is wrong type
        //val n = Foo.syncDAO[FooWithIntId].findOneByID(foo._id).get
    }

    @Test
    def testCustomQueryReturnsVariousEntityTypesWithIntId() {
        val foo = FooWithIntId(100, 23, "woohoo")
        FooWithIntId.caseClassSyncDAO.save(foo)

        val objects = FooWithIntId.customQuery[BObject].toIndexedSeq
        assertEquals(1, objects.size)
        assertEquals(BInt32(23), objects(0).get("intField").get)
        assertEquals(BString("woohoo"), objects(0).get("stringField").get)

        val caseClasses = FooWithIntId.customQuery[FooWithIntId].toIndexedSeq
        assertEquals(1, caseClasses.size)
        val f = caseClasses(0)
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)
    }

    @Test
    def testFindByIDAllResultTypesWithIntId() {
        val foo = FooWithIntId(101, 23, "woohoo")
        FooWithIntId.caseClassSyncDAO.save(foo)

        val o = FooWithIntId.syncDAO[BObject].findOneById(foo._id).get
        assertEquals(BInt32(23), o.get("intField").get)
        assertEquals(BString("woohoo"), o.get("stringField").get)

        val f = FooWithIntId.syncDAO[FooWithIntId].findOneById(foo._id).get
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)
    }
}
