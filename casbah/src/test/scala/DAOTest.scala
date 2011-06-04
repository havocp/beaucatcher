import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.casbah._
import org.bson.types._
import org.junit.Assert._
import org.junit._

package foo {
    case class Foo(_id : ObjectId, intField : Int, stringField : String)

    object Foo extends CasbahCollectionOperationsWithObjectId[Foo] {
        override protected lazy val collection : MongoCollection = {
            CasbahUtil.collection("foo")
        }

        def customQuery[E : Manifest]() = {
            syncDAO[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithIntId(_id : Int, intField : Int, stringField : String)

    object FooWithIntId extends CasbahCollectionOperations[FooWithIntId, Int] {
        override protected lazy val collection : MongoCollection = {
            CasbahUtil.collection("fooWithIntId")
        }

        def customQuery[E : Manifest]() = {
            syncDAO[E].find(BObject("intField" -> 23))
        }
    }
}

class DAOTest {
    import foo._

    @org.junit.Before
    def setup() {
        CasbahUtil.collection("foo").remove(MongoDBObject())
        CasbahUtil.collection("fooWithIntId").remove(MongoDBObject())
    }

    @Test
    def testSaveAndFindOneCaseClass() {
        val foo = Foo(new ObjectId(), 23, "woohoo")
        Foo.caseClassSyncDAO.save(foo)
        val maybeFound = Foo.caseClassSyncDAO.findOneByID(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testSaveAndFindOneCaseClassWithIntId() {
        val foo = FooWithIntId(89, 23, "woohoo")
        FooWithIntId.caseClassSyncDAO.save(foo)
        val maybeFound = FooWithIntId.caseClassSyncDAO.findOneByID(foo._id)
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
}
