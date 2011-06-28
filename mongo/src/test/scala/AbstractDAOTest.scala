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

    trait AbstractFooWithOptionalField extends Product {
        val _id : ObjectId
        val intField : Int
        val stringField : Option[String]
    }
}

import abstractfoo._

abstract class AbstractDAOTest[Foo <: AbstractFoo, FooWithIntId <: AbstractFooWithIntId, FooWithOptionalField <: AbstractFooWithOptionalField](Foo : CollectionOperations[Foo, ObjectId],
    FooWithIntId : CollectionOperations[FooWithIntId, Int],
    FooWithOptionalField : CollectionOperations[FooWithOptionalField, ObjectId])
    extends TestUtils {

    protected def newFoo(id : ObjectId, i : Int, s : String) : Foo
    protected def newFooWithIntId(id : Int, i : Int, s : String) : FooWithIntId
    protected def newFooWithOptionalField(id : ObjectId, i : Int, s : Option[String]) : FooWithOptionalField

    @org.junit.Before
    def setup() {
        Foo.syncDAO.remove(BObject())
        FooWithIntId.syncDAO.remove(BObject())
        FooWithOptionalField.syncDAO.remove(BObject())
    }

    @Test
    def haveProperCollectionNames() = {
        assertEquals("foo", Foo.collectionName)
        assertEquals("fooWithIntId", FooWithIntId.collectionName)
    }

    @Test
    def testSaveAndFindOneCaseClass() {
        val foo = newFoo(new ObjectId(), 23, "woohoo")
        Foo.syncDAO[Foo].save(foo)
        val maybeFound = Foo.syncDAO[Foo].findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testSaveAndFindOneCaseClassWithIntId() {
        val foo = newFooWithIntId(89, 23, "woohoo")
        FooWithIntId.syncDAO[FooWithIntId].save(foo)
        val maybeFound = FooWithIntId.syncDAO[FooWithIntId].findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testFindByIDAllResultTypes() {
        val foo = newFoo(new ObjectId(), 23, "woohoo")
        Foo.syncDAO[Foo].save(foo)

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
        FooWithIntId.syncDAO[FooWithIntId].save(foo)

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
            Foo.syncDAO[Foo].insert(foo)
        }
    }

    private def create1234Optional() {
        for (i <- 1 to 4) {
            val foo = newFooWithOptionalField(new ObjectId(), i, Some(i.toString))
            FooWithOptionalField.syncDAO[FooWithOptionalField].insert(foo)
        }
    }

    private def create1to50() {
        for (i <- 1 to 50) {
            val foo = newFoo(new ObjectId(), i, i.toString)
            Foo.syncDAO[Foo].insert(foo)
        }
    }

    @Test
    def testCountAll() {
        create1234()
        assertEquals(4, Foo.syncDAO[BObject].count())
        assertEquals(4, Foo.syncDAO[Foo].count())
        assertEquals(4, Foo.syncDAO.count())
    }

    @Test
    def testCountAllWithManyObjects() {
        create1to50()
        assertEquals(50, Foo.syncDAO[BObject].count())
        assertEquals(50, Foo.syncDAO[Foo].count())
        assertEquals(50, Foo.syncDAO.count())
    }

    @Test
    def testCountWithQuery() {
        create1234()
        val query = BObject("intField" -> 2)
        assertEquals(1, Foo.syncDAO[Foo].count(query))
        assertEquals(1, Foo.syncDAO[BObject].count(query))
        assertEquals(1, Foo.syncDAO.count(query))
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
        assertEquals(8, Foo.syncDAO.count())

        val allIds = Foo.syncDAO[BObject, BValue].distinct("_id")
        assertEquals(8, allIds.length)
        val allInts = Foo.syncDAO[BObject, BValue].distinct("intField")
        assertEquals(4, allInts.length)
        for (i <- 1 to 4) {
            assertTrue(allInts.find(_.unwrapped == i).isDefined)
        }
        val allStrings = Foo.syncDAO[BObject, BValue].distinct("stringField")
        assertEquals(4, allStrings.length)
        for (i <- 1 to 4) {
            assertTrue(allStrings.find(_.unwrapped == i.toString).isDefined)
        }
    }

    @Test
    def testDistinctWithQuery() {
        create1234()
        create1234()
        assertEquals(8, Foo.syncDAO.count())

        assertEquals(2, Foo.syncDAO.distinct("_id", BObject("intField" -> 3)).length)
        assertEquals(1, Foo.syncDAO.distinct("intField", BObject("intField" -> 3)).length)
    }

    @Test
    def testFindAll() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())

        val allBObject = Foo.syncDAO[BObject].find().toSeq
        assertEquals(4, allBObject.length)
        for (i <- 1 to 4) {
            assertTrue(allBObject.find(_.getUnwrappedAs[Int]("intField") == i).isDefined)
        }

        val allCaseClasses = Foo.syncDAO[Foo].find().toSeq
        assertEquals(4, allCaseClasses.length)
        for (i <- 1 to 4) {
            assertTrue(allCaseClasses.find(_.intField == i).isDefined)
        }
    }

    @Test
    def testFindWithQuery() {
        create1234()
        create1234()
        assertEquals(8, Foo.syncDAO.count())

        val twos = Foo.syncDAO[Foo].find(BObject("intField" -> 2)).toIndexedSeq
        assertEquals(2, twos.length)
        assertEquals(2, twos(0).intField)
        assertEquals(2, twos(1).intField)
    }

    // tests that the AllFields object does the same thing as not passing in
    // a fields object
    @Test
    def testFindWithAllFields() {
        create1234()
        create1234()
        assertEquals(8, Foo.syncDAO.count())

        val twos = Foo.syncDAO[Foo].find(BObject("intField" -> 2), AllFields).toIndexedSeq
        assertEquals(2, twos.length)
        assertEquals(2, twos(0).intField)
        assertEquals(2, twos(1).intField)
        assertEquals("2", twos(0).stringField)
        assertEquals("2", twos(1).stringField)
    }

    @Test
    def testFindWithIncludedFields() {
        create1234()
        create1234()
        assertEquals(8, Foo.syncDAO.count())
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
        assertEquals(8, Foo.syncDAO.count())

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
        assertEquals(8, Foo.syncDAO.count())

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
        assertEquals(8, Foo.syncDAO.count())
        val e = intercept[Exception] {
            val threes = Foo.syncDAO[Foo].find(BObject("intField" -> 3),
                IncludedFields("intField")).toIndexedSeq
        }
        assertTrue(e.getMessage.contains("requires value"))
    }

    @Test
    def testFindWithFieldsWorksOnCaseClassWithOptional() {
        create1234Optional()
        create1234Optional()
        assertEquals(8, FooWithOptionalField.syncDAO.count())

        val threesWithout = FooWithOptionalField.syncDAO[FooWithOptionalField].find(BObject("intField" -> 3),
            IncludedFields("intField")).toIndexedSeq
        assertEquals(2, threesWithout.length)
        assertEquals(3, threesWithout(0).intField)
        assertEquals(3, threesWithout(1).intField)
        assertTrue(threesWithout(0).stringField.isEmpty)
        assertTrue(threesWithout(1).stringField.isEmpty)

        val threesWith = FooWithOptionalField.syncDAO[FooWithOptionalField].find(BObject("intField" -> 3),
            IncludedFields("intField", "stringField")).toIndexedSeq
        assertEquals(2, threesWith.length)
        assertEquals(3, threesWith(0).intField)
        assertEquals(3, threesWith(1).intField)
        assertFalse(threesWith(0).stringField.isEmpty)
        assertFalse(threesWith(1).stringField.isEmpty)
        assertEquals("3", threesWith(0).stringField.get)
        assertEquals("3", threesWith(1).stringField.get)
    }

    @Test
    def testFindWithSkip() {
        create1234()
        create1234()
        assertEquals(8, Foo.syncDAO.count())

        for (i <- 1 to 8) {
            val found = Foo.syncDAO[BObject].find(BObject(), AllFields,
                i, /* skip */
                -1, /* limit */
                0 /* batch size */ ).toSeq
            assertEquals(8 - i, found.length)
        }
    }

    @Test
    def testFindWithLimit() {
        create1234()
        create1234()
        assertEquals(8, Foo.syncDAO.count())

        for (i <- 1 to 8) {
            val found = Foo.syncDAO[BObject].find(BObject(), AllFields,
                0, /* skip */
                i, /* limit */
                0 /* batch size */ ).toSeq
            assertEquals(i, found.length)
        }
    }

    @Test
    def testFindOne() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())
        val found = Foo.syncDAO[Foo].findOne()
        assertTrue(found.isDefined)
    }

    @Test
    def testFindOneWithQuery() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())
        val found = Foo.syncDAO[Foo].findOne(BObject("intField" -> 3))
        assertTrue(found.isDefined)
        assertEquals(3, found.get.intField)
    }

    @Test
    def testFindOneWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.syncDAO.count())
        val found = FooWithOptionalField.syncDAO[FooWithOptionalField].findOne(BObject("intField" -> 3),
            IncludedFields("intField"))
        assertTrue(found.isDefined)
        assertEquals(3, found.get.intField)
        assertTrue(found.get.stringField.isEmpty)
    }

    @Test
    def testFindOneById() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())
        val found = Foo.syncDAO[Foo].findOne()
        assertTrue(found.isDefined)

        // now refind it by id
        val foundById = Foo.syncDAO[Foo].findOneById(found.get._id)
        assertTrue(foundById.isDefined)
        assertEquals(found, foundById)
    }

    @Test
    def testFindOneByIdWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.syncDAO.count())
        val found = FooWithOptionalField.syncDAO[FooWithOptionalField].findOne(BObject("intField" -> 3),
            IncludedFields("intField"))
        assertTrue(found.isDefined)
        assertEquals(3, found.get.intField)
        assertTrue(found.get.stringField.isEmpty)

        // now refind it by id
        val foundById = FooWithOptionalField.syncDAO[FooWithOptionalField].findOneById(found.get._id,
            IncludedFields("intField"))
        assertTrue(foundById.isDefined)
        assertEquals(3, foundById.get.intField)
        assertTrue(foundById.get.stringField.isEmpty)
        assertEquals(found, foundById)
    }

    @Test
    def testFindAndReplace() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())

        val old = Foo.syncDAO[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one
        val stillOld = Foo.syncDAO[Foo].findAndReplace(BObject("intField" -> 3),
            newFoo(old.get._id, 42, "42"))
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertEquals(4, Foo.syncDAO.count())
        assertEquals(old.get._id, stillOld.get._id)

        // but we wrote out the new one
        val replaced = Foo.syncDAO[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(42, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)

        // now pass in the flag to return the new one and check that works
        val nowNew = Foo.syncDAO[Foo].findAndReplace(BObject("intField" -> 42),
            newFoo(old.get._id, 43, "43"), Set[FindAndModifyFlag](FindAndModifyNew))
        assertTrue(nowNew.isDefined)
        assertEquals(43, nowNew.get.intField)
        assertEquals(old.get._id, nowNew.get._id)
    }

    @Test
    def testFindAndReplaceWithSort() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())

        // in natural order, we're expecting 1, 2, 3, 4
        // since we insert them that way (not sure if mongo
        // guarantees this?)
        val firstNatural = Foo.syncDAO[Foo].findOne()
        assertTrue(firstNatural.isDefined)
        assertEquals(1, firstNatural.get.intField)

        val sortedByIntBackward = Foo.syncDAO[Foo].find(BObject("query" -> BObject(),
            "orderby" -> BObject("intField" -> -1)), AllFields, 0, 1, 0).toSeq
        assertTrue(!sortedByIntBackward.isEmpty)
        val last = sortedByIntBackward.last

        assertEquals(4, last.intField)

        // replace the last item sorting backward by intField
        val old = Foo.syncDAO[Foo].findAndReplace(BObject(),
            newFoo(last._id, 42, "42"),
            BObject("intField" -> -1))

        assertTrue(old.isDefined)
        assertEquals(4, old.get.intField)
        assertEquals(4, Foo.syncDAO.count())
        assertEquals(last._id, old.get._id)
    }

    @Test
    def testFindAndReplaceWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.syncDAO.count())

        val old = FooWithOptionalField.syncDAO[FooWithOptionalField].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one minus excluded field
        val stillOld = FooWithOptionalField.syncDAO[FooWithOptionalField].findAndReplace(BObject("intField" -> 3),
            newFooWithOptionalField(old.get._id, 42, Some("42")), BObject(), ExcludedFields("stringField"), Set())
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertTrue(stillOld.get.stringField.isEmpty)
        assertEquals(4, FooWithOptionalField.syncDAO.count())
        assertEquals(old.get._id, stillOld.get._id)
    }

    @Test
    def testFindAndReplaceIgnoresId() {
        create1234()
        assertEquals(4, Foo.syncDAO.count())

        val old = Foo.syncDAO[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one
        val stillOld = Foo.syncDAO[Foo].findAndReplace(BObject("intField" -> 3),
            // this new ObjectId() is the thing that needs to get ignored
            newFoo(new ObjectId(), 42, "42"))
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertEquals(4, Foo.syncDAO.count())
        assertEquals(old.get._id, stillOld.get._id)

        // but we wrote out the new one and it has the old id
        val replaced = Foo.syncDAO[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(42, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)
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
        assertEquals(0, Foo.syncDAO.count())
        val f = newFoo(new ObjectId(), 14, "14")
        Foo.syncDAO[Foo].insert(f)
        assertEquals(1, Foo.syncDAO.count())
        val foundById = Foo.syncDAO[Foo].findOneById(f._id)
        assertTrue(foundById.isDefined)
        assertEquals(14, foundById.get.intField)

        // duplicate should throw
        val e = intercept[Exception] {
            Foo.syncDAO[Foo].insert(f)
        }
        assertTrue(e.getMessage.contains("duplicate key"))
        assertTrue(e.getMessage.contains("E11000"))
    }

    @Test
    def testSave() {
        assertEquals(0, Foo.syncDAO.count())
        val f = newFoo(new ObjectId(), 14, "14")
        Foo.syncDAO[Foo].save(f)
        assertEquals(1, Foo.syncDAO.count())
        val foundById = Foo.syncDAO[Foo].findOneById(f._id)
        assertTrue(foundById.isDefined)
        assertEquals(14, foundById.get.intField)

        // duplicate should not throw on a save
        Foo.syncDAO[Foo].save(f)
        assertEquals(1, Foo.syncDAO.count())
        val foundById2 = Foo.syncDAO[Foo].findOneById(f._id)
        assertTrue(foundById2.isDefined)
        assertEquals(14, foundById2.get.intField)

        // making a change should be possible with a save
        val f2 = newFoo(f._id, 15, "15")
        Foo.syncDAO[Foo].save(f2)
        assertEquals(1, Foo.syncDAO.count())
        val foundById3 = Foo.syncDAO[Foo].findOneById(f2._id)
        assertTrue(foundById3.isDefined)
        assertEquals(15, foundById3.get.intField)
        assertEquals(f._id, foundById3.get._id)
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
