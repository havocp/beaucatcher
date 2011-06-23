import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.types._
import org.junit.Assert._
import org.junit._

package abstractfoo {
    trait AbstractFoo extends Product {
        val _id : ObjectId
        val intField : Int
        val stringField : String
    }

    trait AbstractFooWithIntId extends Product {
        val _id : Int
        val intField : Int
        val stringField : String
    }
}

import abstractfoo._

abstract class AbstractDAOTest[Foo <: AbstractFoo, FooWithIntId <: AbstractFooWithIntId](Foo : CollectionOperations[Foo, ObjectId], FooWithIntId : CollectionOperations[FooWithIntId, Int])
    extends TestUtils {

    protected def newFoo(id : ObjectId, i : Int, s : String) : Foo
    protected def newFooWithIntId(id : Int, i : Int, s : String) : FooWithIntId

    @org.junit.Before
    def setup() {
        Foo.bobjectSyncDAO.remove(BObject())
        FooWithIntId.bobjectSyncDAO.remove(BObject())
    }

    @Test
    def haveProperCollectionNames() = {
        assertEquals("foo", Foo.collectionName)
        assertEquals("fooWithIntId", FooWithIntId.collectionName)
    }

    @Test
    def testSaveAndFindOneCaseClass() {
        val foo = newFoo(new ObjectId(), 23, "woohoo")
        Foo.caseClassSyncDAO.save(foo)
        val maybeFound = Foo.caseClassSyncDAO.findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testSaveAndFindOneCaseClassWithIntId() {
        val foo = newFooWithIntId(89, 23, "woohoo")
        FooWithIntId.caseClassSyncDAO.save(foo)
        val maybeFound = FooWithIntId.caseClassSyncDAO.findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testFindByIDAllResultTypes() {
        val foo = newFoo(new ObjectId(), 23, "woohoo")
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
    def testFindByIDAllResultTypesWithIntId() {
        val foo = newFooWithIntId(101, 23, "woohoo")
        FooWithIntId.caseClassSyncDAO.save(foo)

        val o = FooWithIntId.syncDAO[BObject].findOneById(foo._id).get
        assertEquals(BInt32(23), o.get("intField").get)
        assertEquals(BString("woohoo"), o.get("stringField").get)

        val f = FooWithIntId.syncDAO[FooWithIntId].findOneById(foo._id).get
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)
    }

    @Test
    def testEmptyQuery() {
        val e1 = Foo.syncDAO[BObject].emptyQuery
        assertEquals(BObject.empty, e1)
        val e2 = Foo.syncDAO[Foo].emptyQuery
        assertEquals(BObject.empty, e2)
    }

    private def create1234() {
        for (i <- 1 to 4) {
            val foo = newFoo(new ObjectId(), i, i.toString)
            Foo.caseClassSyncDAO.insert(foo)
        }
    }

    private def create1to50() {
        for (i <- 1 to 50) {
            val foo = newFoo(new ObjectId(), i, i.toString)
            Foo.caseClassSyncDAO.insert(foo)
        }
    }

    @Test
    def testCountAll() {
        create1234()
        assertEquals(4, Foo.bobjectSyncDAO.count())
        assertEquals(4, Foo.caseClassSyncDAO.count())
    }

    @Test
    def testCountAllWithManyObjects() {
        create1to50()
        assertEquals(50, Foo.bobjectSyncDAO.count())
        assertEquals(50, Foo.caseClassSyncDAO.count())
    }

    @Test
    def testCountWithQuery() {
        create1234()
        val query = BObject("intField" -> 2)
        assertEquals(1, Foo.bobjectSyncDAO.count(query))
        assertEquals(1, Foo.caseClassSyncDAO.count(query))
    }

    @Test
    def testCountWithFields() {
        // FIXME wtf does fields do on count() ? Casbah has a version of count() with fields,
        // that's the only evidence I've found that this should exist
    }

    @Test
    def testDistinct() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())

        val allIds = Foo.bobjectSyncDAO.distinct("_id")
        assertEquals(8, allIds.length)
        val allInts = Foo.bobjectSyncDAO.distinct("intField")
        assertEquals(4, allInts.length)
        for (i <- 1 to 4) {
            assertTrue(allInts.find(_.unwrapped == i).isDefined)
        }
        val allStrings = Foo.bobjectSyncDAO.distinct("stringField")
        assertEquals(4, allStrings.length)
        for (i <- 1 to 4) {
            assertTrue(allStrings.find(_.unwrapped == i.toString).isDefined)
        }
    }

    @Test
    def testDistinctWithQuery() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())

        assertEquals(2, Foo.bobjectSyncDAO.distinct("_id", BObject("intField" -> 3)).length)
        assertEquals(1, Foo.bobjectSyncDAO.distinct("intField", BObject("intField" -> 3)).length)
    }

    @Test
    def testFindAll() {
        create1234()
        assertEquals(4, Foo.bobjectSyncDAO.count())

        val allBObject = Foo.bobjectSyncDAO.find().toSeq
        assertEquals(4, allBObject.length)
        for (i <- 1 to 4) {
            assertTrue(allBObject.find(_.getUnwrappedAs[Int]("intField") == i).isDefined)
        }

        val allCaseClasses = Foo.caseClassSyncDAO.find().toSeq
        assertEquals(4, allCaseClasses.length)
        for (i <- 1 to 4) {
            assertTrue(allCaseClasses.find(_.intField == i).isDefined)
        }
    }

    @Test
    def testFindWithQuery() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())

        val twos = Foo.syncDAO[Foo].find(BObject("intField" -> 2)).toIndexedSeq
        assertEquals(2, twos.length)
        assertEquals(2, twos(0).intField)
        assertEquals(2, twos(1).intField)
    }

    @Test
    def testFindWithIncludedFields() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())
        val threes = Foo.syncDAO[BObject].find(BObject("intField" -> 3),
            IncludedFields("intField")).toIndexedSeq
        assertEquals(2, threes.length)
        assertEquals(3, threes(0).getUnwrappedAs[Int]("intField"))
        assertEquals(3, threes(1).getUnwrappedAs[Int]("intField"))
        // _id is included by default
        assertTrue(threes(0).contains("_id"))
        assertTrue(threes(1).contains("_id"))
        // we shouldn't get a field we didn't ask for
        assertFalse(threes(0).contains("stringField"))
        assertFalse(threes(1).contains("stringField"))
    }

    @Test
    def testFindWithIncludedFieldsWithoutId() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())

        val threes = Foo.syncDAO[BObject].find(BObject("intField" -> 3),
            IncludedFields(WithoutId, "intField")).toIndexedSeq
        assertEquals(2, threes.length)
        assertEquals(3, threes(0).getUnwrappedAs[Int]("intField"))
        assertEquals(3, threes(1).getUnwrappedAs[Int]("intField"))
        // _id was explicitly excluded
        assertFalse(threes(0).contains("_id"))
        assertFalse(threes(1).contains("_id"))
        // we shouldn't get a field we didn't ask for
        assertFalse(threes(0).contains("stringField"))
        assertFalse(threes(1).contains("stringField"))
    }

    @Test
    def testFindWithExcludedFields() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())

        val threes = Foo.syncDAO[BObject].find(BObject("intField" -> 3),
            ExcludedFields("intField")).toIndexedSeq
        assertEquals(2, threes.length)
        assertEquals(3.toString, threes(0).getUnwrappedAs[String]("stringField"))
        assertEquals(3.toString, threes(1).getUnwrappedAs[String]("stringField"))
        assertTrue(threes(0).contains("_id"))
        assertTrue(threes(1).contains("_id"))
        // we shouldn't get a field we excluded
        assertFalse(threes(0).contains("intField"))
        assertFalse(threes(1).contains("intField"))
    }

    @Test
    def testFindWithFieldsFailsOnCaseClass() {
        create1234()
        create1234()
        assertEquals(8, Foo.bobjectSyncDAO.count())
        val e = intercept[Exception] {
            val threes = Foo.syncDAO[Foo].find(BObject("intField" -> 3),
                IncludedFields("intField")).toIndexedSeq
        }
        assertTrue(e.getMessage.contains("requires value"))
    }

    @Test
    def testFindWithSkip() {

    }

    @Test
    def testFindWithLimit() {

    }

    @Test
    def testFindOne() {

    }

    @Test
    def testFindOneWithQuery() {

    }

    @Test
    def testFindOneWithFields() {

    }

    @Test
    def testFindOneById() {

    }

    @Test
    def testFindOneByIdWithFields() {

    }

    @Test
    def testFindAndReplace() {

    }

    @Test
    def testFindAndReplaceWithSort() {

    }

    @Test
    def testFindAndReplaceWithFields() {

    }

    @Test
    def testFindAndModify() {

    }

    @Test
    def testFindAndModifyWithSort() {

    }

    @Test
    def testFindAndModifyWithFields() {

    }

    @Test
    def testFindAndRemove() {

    }

    @Test
    def testFindAndRemoveWithSort() {

    }

    @Test
    def testInsert() {

    }

    @Test
    def testSave() {

    }

    @Test
    def testUpdate() {

    }

    @Test
    def testUpdateUpsert() {

    }

    @Test
    def testUpdateMulti() {

    }

    @Test
    def testRemove() {

    }

    @Test
    def testRemoveById() {

    }
}
