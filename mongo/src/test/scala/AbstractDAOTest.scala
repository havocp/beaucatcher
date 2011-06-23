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

abstract class AbstractDAOTest[Foo <: AbstractFoo, FooWithIntId <: AbstractFooWithIntId](Foo : CollectionOperations[Foo, ObjectId], FooWithIntId : CollectionOperations[FooWithIntId, Int]) {

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

    }

    @Test
    def testCountAll() {

    }

    @Test
    def testCountWithQuery() {

    }

    @Test
    def testCountWithFields() {

    }

    @Test
    def testDistinct() {

    }

    @Test
    def testDistinctWithQuery() {

    }

    @Test
    def testFindAll() {

    }

    @Test
    def testFindWithQuery() {

    }

    @Test
    def testFindWithFields() {

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
