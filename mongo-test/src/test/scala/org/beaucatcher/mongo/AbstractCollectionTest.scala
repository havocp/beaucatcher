package org.beaucatcher.mongo

import org.beaucatcher.bobject.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.bobject._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._

package foo {
    import BObjectCodecs._

    case class Foo(_id: ObjectId, intField: Int, stringField: String)

    object Foo extends CollectionAccessWithEntitiesBObjectOrCaseClassIdObjectId[Foo] {
        def customQuery[E](implicit context: Context, provider: CodecSetProvider[E, ErrorIfDecodedValue, CollectionAccessWithTwoEntityTypes[BObject, ObjectId, BObject, BValue, Foo, Any], BObject, E, ObjectId]) = {
            sync[E].find(BObject("intField" -> 23))
        }

        override def migrate(implicit context: Context): Unit = {
            // we're mostly doing this just to test that migrate() gets called
            // and works
            sync.ensureIndex(BObject("intField" -> 1))
        }
    }

    case class FooWithIntId(_id: Int, intField: Int, stringField: String)

    object FooWithIntId extends CollectionAccessWithEntitiesBObjectOrCaseClass[FooWithIntId, Int] {
        def customQuery[E](implicit context: Context, provider: CodecSetProvider[E, ErrorIfDecodedValue, CollectionAccessWithTwoEntityTypes[BObject, Int, BObject, BValue, FooWithIntId, Any], BObject, E, Int]) = {
            sync[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithOptionalField(_id: ObjectId, intField: Int, stringField: Option[String])

    object FooWithOptionalField extends CollectionAccessWithEntitiesBObjectOrCaseClassIdObjectId[FooWithOptionalField] {
    }

    object Bar extends CollectionAccessWithEntityBObjectIdObjectId {

    }
}

abstract class AbstractCollectionTest()
    extends TestUtils {

    self: ContextProvider =>

    import foo._

    protected implicit def context: Context = mongoContext

    @org.junit.Before
    def setup() {
        Foo.sync.removeAll()
        FooWithIntId.sync.removeAll()
        FooWithOptionalField.sync.removeAll()
        Bar.sync.removeAll()
        Foo.sync.dropIndexes()
        FooWithIntId.sync.dropIndexes()
        FooWithOptionalField.sync.dropIndexes()
        Bar.sync.dropIndexes()
    }

    @Test
    def testOneEntityTypeCompiles(): Unit = {
        // there's no runtime test here, just checking all this junk compiles.
        abstract class TestOne[IdType]()(implicit idEncoder: IdEncoder[IdType])
            extends CollectionAccessWithEntityBObject[IdType]() {

            // TODO move to test suite (test all this compiles)
            private def testSync()(implicit context: Context) = {
                sync.count()
                val fs = sync.find()
                val bs = sync[BObject].find()
                val bs2 = sync[BObject, BValue].find()

                val bs3 = sync[BObject].find(BObject.empty)

                val vs1 = sync[BObject, BValue].distinct("foo")
            }

            // TODO move to test suite (test all this compiles)
            private def testAsync()(implicit context: Context) = {
                async.count()
                val fs = async.find()
                val bs = async[BObject].find()
                val bs2 = async[BObject, BValue].find()

                val bs3 = async[BObject].find(BObject.empty)

                val vs1 = async[BObject, BValue].distinct("foo")
            }
        }
    }

    @Test
    def testTwoEntityTypesCompiles(): Unit = {
        // there's no runtime test here, just checking all this junk compiles.

        class TestTwo[EntityType <: Product, IdType](implicit entityManifest: Manifest[EntityType],
            idEncoder: IdEncoder[IdType])
            extends CollectionAccessWithEntitiesBObjectOrCaseClass[EntityType, IdType] {
            val p1 = CollectionAccessWithTwoEntityTypes.firstCodecSetProviderEV[BObject, IdType, BObject, BValue, EntityType, Any, this.type]
            val p2 = CollectionAccessWithTwoEntityTypes.secondCodecSetProviderEV[BObject, IdType, EntityType, Any, BObject, BValue, this.type]

            def getProvider1()(implicit p: CodecSetProvider[BObject, BValue, CollectionAccessWithTwoEntityTypes[BObject, IdType, BObject, BValue, EntityType, Any], BObject, BObject, IdType]) = p
            val provider1 = getProvider1()

            def getCodecSet1()(implicit p: CodecSetProvider[BObject, BValue, CollectionAccessWithTwoEntityTypes[BObject, IdType, BObject, BValue, EntityType, Any], BObject, BObject, IdType]): CollectionCodecSet[BObject, BObject, BObject, IdType, BValue] =
                p.codecSet(this)
            val c1 = getCodecSet1()

            def getProvider2()(implicit p: CodecSetProvider[EntityType, Any, CollectionAccessWithTwoEntityTypes[BObject, IdType, BObject, BValue, EntityType, Any], BObject, EntityType, IdType]) = p
            val provider2 = getProvider2()

            def getCodecSet2()(implicit p: CodecSetProvider[EntityType, Any, CollectionAccessWithTwoEntityTypes[BObject, IdType, BObject, BValue, EntityType, Any], BObject, EntityType, IdType]): CollectionCodecSet[BObject, EntityType, EntityType, IdType, Any] =
                p.codecSet(this)
            val c2 = getCodecSet2()

            private def testSync()(implicit context: Context) = {
                sync.count()
                val fs = sync.find()
                val bs = sync[BObject].find()
                val bs2 = sync[BObject, BValue].find()
                val es = sync[EntityType].find()
                val es2 = sync[EntityType, Any].find()

                val bs3 = sync[BObject].find(BObject.empty)

                val vs1 = sync[BObject, BValue].distinct("foo")
                val vs2 = sync[EntityType, Any].distinct("foo")

                // this would just throw at runtime, but we just need a value with static type EntityType
                val e: EntityType = secondCodecSet.collectionQueryResultDecoder.decodeIterator(Iterator.empty)

                sync[EntityType].save(e)
                // "sync" with no type param is assumed to mean the first entity type for saves
                sync.save(BObject.empty)
            }

            private def testAsync()(implicit context: Context) = {
                async.count()
                val fs = async.find()
                val bs = async[BObject].find()
                val bs2 = async[BObject, BValue].find()
                val es = async[EntityType].find()
                val es2 = async[EntityType, Any].find()

                val bs3 = async[BObject].find(BObject.empty)

                val vs1 = async[BObject, BValue].distinct("foo")
                val vs2 = async[EntityType, Any].distinct("foo")

                // this would just throw at runtime, but we just need a value with static type EntityType
                val e: EntityType = secondCodecSet.collectionQueryResultDecoder.decodeIterator(Iterator.empty)

                async[EntityType].save(e)
                // "sync" with no type param is assumed to mean the first entity type for saves
                async.save(BObject.empty)
            }
        }
    }

    @Test
    def haveProperCollectionNames() = {
        assertEquals("foo", Foo.collectionName)
        assertEquals("fooWithIntId", FooWithIntId.collectionName)
        assertEquals(Foo.collectionName, Foo.sync.name)
        assertEquals(FooWithIntId.collectionName, FooWithIntId.sync.name)
        assertEquals(Bar.collectionName, Bar.sync.name)
    }

    @Test
    def testFullName() = {
        assertEquals(context.database.name + "." + Foo.collectionName, Foo.sync.fullName)
        assertEquals(context.database.name + "." + Bar.collectionName, Bar.sync.fullName)
    }

    @Test
    def testSaveAndFindOneCaseClass() {
        val foo = Foo(ObjectId(), 23, "woohoo")
        Foo.sync[Foo].save(foo)
        val maybeFound = Foo.sync[Foo].findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testSaveAndFindOneCaseClassWithIntId() {
        val foo = FooWithIntId(89, 23, "woohoo")
        FooWithIntId.sync[FooWithIntId].save(foo)
        val maybeFound = FooWithIntId.sync[FooWithIntId].findOneById(foo._id)
        assertTrue(maybeFound.isDefined)
        assertEquals(foo, maybeFound.get)
    }

    @Test
    def testFindByIDAllResultTypes() {
        val foo = Foo(ObjectId(), 23, "woohoo")
        Foo.sync[Foo].save(foo)

        val o = Foo.sync[BObject].findOneById(foo._id).get
        assertEquals(BInt32(23), o.get("intField").get)
        assertEquals(BString("woohoo"), o.get("stringField").get)

        val f = Foo.sync[Foo].findOneById(foo._id).get
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)

        // should not compile because FooWithIntId is wrong type
        //val n = Foo.sync[FooWithIntId].findOneByID(foo._id).get
    }

    @Test
    def testFindByIDAllResultTypesWithIntId() {
        val foo = FooWithIntId(101, 23, "woohoo")
        FooWithIntId.sync[FooWithIntId].save(foo)

        val o = FooWithIntId.sync[BObject].findOneById(foo._id).get
        assertEquals(BInt32(23), o.get("intField").get)
        assertEquals(BString("woohoo"), o.get("stringField").get)

        val f = FooWithIntId.sync[FooWithIntId].findOneById(foo._id).get
        assertEquals(23, f.intField)
        assertEquals("woohoo", f.stringField)
    }

    private def create1234() {
        for (i <- 1 to 4) {
            val foo = Foo(ObjectId(), i, i.toString)
            Foo.sync[Foo].insert(foo)
        }
    }

    private def create2143() {
        for (i <- Seq(2, 1, 4, 3)) {
            val foo = Foo(ObjectId(), i, i.toString)
            Foo.sync[Foo].insert(foo)
        }
    }

    private def create1234Optional() {
        for (i <- 1 to 4) {
            val foo = FooWithOptionalField(ObjectId(), i, Some(i.toString))
            FooWithOptionalField.sync[FooWithOptionalField].insert(foo)
        }
    }

    private def create1to50() {
        for (i <- 1 to 50) {
            val foo = Foo(ObjectId(), i, i.toString)
            Foo.sync[Foo].insert(foo)
        }
    }

    @Test
    def testCountAll() {
        create1234()
        assertEquals(4, Foo.sync[BObject].count())
        assertEquals(4, Foo.sync[Foo].count())
        assertEquals(4, Foo.sync.count())
    }

    @Test
    def testCountAllWithManyObjects() {
        create1to50()
        assertEquals(50, Foo.sync[BObject].count())
        assertEquals(50, Foo.sync[Foo].count())
        assertEquals(50, Foo.sync.count())
    }

    @Test
    def testCountWithQuery() {
        create1234()
        val query = BObject("intField" -> 2)
        assertEquals(1, Foo.sync[Foo].count(query))
        assertEquals(1, Foo.sync[BObject].count(query))
        assertEquals(1, Foo.sync.count(query))
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
        assertEquals(8, Foo.sync.count())

        val allIds = Foo.sync[BObject, BValue].distinct("_id").toSeq
        assertEquals(8, allIds.length)
        val allInts = Foo.sync[BObject, BValue].distinct("intField").toSeq
        assertEquals(4, allInts.length)
        for (i <- 1 to 4) {
            assertTrue(allInts.find(_.unwrapped == i).isDefined)
        }
        val allStrings = Foo.sync[BObject, BValue].distinct("stringField").toSeq
        assertEquals(4, allStrings.length)
        for (i <- 1 to 4) {
            assertTrue(allStrings.find(_.unwrapped == i.toString).isDefined)
        }
    }

    @Test
    def testDistinctWithQuery() {
        create1234()
        create1234()
        assertEquals(8, Foo.sync.count())

        assertEquals(2, Foo.sync.distinct("_id", BObject("intField" -> 3)).length)
        assertEquals(1, Foo.sync.distinct("intField", BObject("intField" -> 3)).length)
    }

    @Test
    def testFindAll() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val allBObject = Foo.sync[BObject].find().toSeq
        assertEquals(4, allBObject.length)
        for (i <- 1 to 4) {
            assertTrue(allBObject.find(_.getUnwrappedAs[Int]("intField") == i).isDefined)
        }

        val allCaseClasses = Foo.sync[Foo].find().toSeq
        assertEquals(4, allCaseClasses.length)
        for (i <- 1 to 4) {
            assertTrue(allCaseClasses.find(_.intField == i).isDefined)
        }
    }

    @Test
    def testFindWithQuery() {
        create1234()
        create1234()
        assertEquals(8, Foo.sync.count())

        val twos = Foo.sync[Foo].find(BObject("intField" -> 2)).toIndexedSeq
        assertEquals(2, twos.length)
        assertEquals(2, twos(0).intField)
        assertEquals(2, twos(1).intField)
    }

    @Test
    def testFindWithOrderBy() {
        create2143()
        create2143()
        assertEquals(8, Foo.sync.count())

        val all = Foo.sync[Foo].find(BObject("query" -> BObject(),
            "orderby" -> BObject("intField" -> 1))).toIndexedSeq
        assertEquals(8, all.length)
        val expected = 1 to 4 flatMap { x => IndexedSeq(x, x) }
        val actual = all map { _.intField }
        assertEquals(expected, actual)

        val allBackward = Foo.sync[Foo].find(BObject("query" -> BObject(),
            "orderby" -> BObject("intField" -> -1))).toIndexedSeq
        assertEquals(8, all.length)
        val expectedBackward = 1 to 4 flatMap { x => IndexedSeq(x, x) }
        val actualBackward = all map { _.intField }
        assertEquals(expectedBackward, actualBackward)
    }

    // tests that the AllFields object does the same thing as not passing in
    // a fields object
    @Test
    def testFindWithAllFields() {
        create1234()
        create1234()
        assertEquals(8, Foo.sync.count())

        val twos = Foo.sync[Foo].find(BObject("intField" -> 2), AllFields).toIndexedSeq
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
        assertEquals(8, Foo.sync.count())
        val threes = Foo.sync[BObject].find(BObject("intField" -> 3),
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
        assertEquals(8, Foo.sync.count())

        val threes = Foo.sync[BObject].find(BObject("intField" -> 3),
            IncludedFields(FieldsWithoutId, "intField")).toIndexedSeq
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
        assertEquals(8, Foo.sync.count())

        val threes = Foo.sync[BObject].find(BObject("intField" -> 3),
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
        assertEquals(8, Foo.sync.count())
        val e = intercept[Exception] {
            val threes = Foo.sync[Foo].find(BObject("intField" -> 3),
                IncludedFields("intField")).toIndexedSeq
        }
        assertTrue(e.getMessage.contains("requires value"))
    }

    @Test
    def testFindWithFieldsWorksOnCaseClassWithOptional() {
        create1234Optional()
        create1234Optional()
        assertEquals(8, FooWithOptionalField.sync.count())

        val threesWithout = FooWithOptionalField.sync[FooWithOptionalField].find(BObject("intField" -> 3),
            IncludedFields("intField")).toIndexedSeq
        assertEquals(2, threesWithout.length)
        assertEquals(3, threesWithout(0).intField)
        assertEquals(3, threesWithout(1).intField)
        assertTrue(threesWithout(0).stringField.isEmpty)
        assertTrue(threesWithout(1).stringField.isEmpty)

        val threesWith = FooWithOptionalField.sync[FooWithOptionalField].find(BObject("intField" -> 3),
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
        assertEquals(8, Foo.sync.count())

        for (i <- 1 to 8) {
            val found = Foo.sync[BObject].find(BObject(), AllFields,
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
        assertEquals(8, Foo.sync.count())

        for (i <- 1 to 8) {
            val found = Foo.sync[BObject].find(BObject(), AllFields,
                0, /* skip */
                i, /* limit */
                0 /* batch size */ ).toSeq
            assertEquals(i, found.length)
        }
    }

    @Test
    def testFindOne() {
        create1234()
        assertEquals(4, Foo.sync.count())
        val found = Foo.sync[Foo].findOne()
        assertTrue(found.isDefined)
    }

    @Test
    def testFindOneWithQuery() {
        create1234()
        assertEquals(4, Foo.sync.count())
        val found = Foo.sync[Foo].findOne(BObject("intField" -> 3))
        assertTrue(found.isDefined)
        assertEquals(3, found.get.intField)
    }

    @Test
    def testFindOneWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.sync.count())
        val found = FooWithOptionalField.sync[FooWithOptionalField].findOne(BObject("intField" -> 3),
            IncludedFields("intField"))
        assertTrue(found.isDefined)
        assertEquals(3, found.get.intField)
        assertTrue(found.get.stringField.isEmpty)
    }

    @Test
    def testFindOneById() {
        create1234()
        assertEquals(4, Foo.sync.count())
        val found = Foo.sync[Foo].findOne()
        assertTrue(found.isDefined)

        // now refind it by id
        val foundById = Foo.sync[Foo].findOneById(found.get._id)
        assertTrue(foundById.isDefined)
        assertEquals(found, foundById)
    }

    @Test
    def testFindOneByIdWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.sync.count())
        val found = FooWithOptionalField.sync[FooWithOptionalField].findOne(BObject("intField" -> 3),
            IncludedFields("intField"))
        assertTrue(found.isDefined)
        assertEquals(3, found.get.intField)
        assertTrue(found.get.stringField.isEmpty)

        // now refind it by id
        val foundById = FooWithOptionalField.sync[FooWithOptionalField].findOneById(found.get._id,
            IncludedFields("intField"))
        assertTrue(foundById.isDefined)
        assertEquals(3, foundById.get.intField)
        assertTrue(foundById.get.stringField.isEmpty)
        assertEquals(found, foundById)
    }

    @Test
    def testFindAndReplace() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val old = Foo.sync[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one
        val stillOld = Foo.sync[Foo].findAndReplace(BObject("intField" -> 3),
            Foo(old.get._id, 42, "42"))
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertEquals(4, Foo.sync.count())
        assertEquals(old.get._id, stillOld.get._id)

        // but we wrote out the new one
        val replaced = Foo.sync[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(42, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)

        // now pass in the flag to return the new one and check that works
        val nowNew = Foo.sync[Foo].findAndReplace(BObject("intField" -> 42),
            Foo(old.get._id, 43, "43"), Set[FindAndModifyFlag](FindAndModifyNew))
        assertTrue(nowNew.isDefined)
        assertEquals(43, nowNew.get.intField)
        assertEquals(old.get._id, nowNew.get._id)
    }

    @Test
    def testFindAndReplaceWithSort() {
        create1234()
        assertEquals(4, Foo.sync.count())

        // in natural order, we're expecting 1, 2, 3, 4
        // since we insert them that way (not sure if mongo
        // guarantees this?)
        val firstNatural = Foo.sync[Foo].findOne()
        assertTrue(firstNatural.isDefined)
        assertEquals(1, firstNatural.get.intField)

        val sortedByIntBackward = Foo.sync[Foo].find(BObject("query" -> BObject(),
            "orderby" -> BObject("intField" -> -1)), AllFields, 0, 1, 0).toSeq
        assertTrue(!sortedByIntBackward.isEmpty)
        val last = sortedByIntBackward.last

        assertEquals(4, last.intField)

        // replace the last item sorting backward by intField
        val old = Foo.sync[Foo].findAndReplace(BObject(),
            Foo(last._id, 42, "42"),
            BObject("intField" -> -1))

        assertTrue(old.isDefined)
        assertEquals(4, old.get.intField)
        assertEquals(4, Foo.sync.count())
        assertEquals(last._id, old.get._id)

        // but we wrote out the new one and it has the old id and new field
        val replaced = Foo.sync[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(42, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)
    }

    @Test
    def testFindAndReplaceWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.sync.count())

        val old = FooWithOptionalField.sync[FooWithOptionalField].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one minus excluded field
        val stillOld = FooWithOptionalField.sync[FooWithOptionalField].findAndReplace(BObject("intField" -> 3),
            FooWithOptionalField(old.get._id, 42, Some("42")), BObject(), ExcludedFields("stringField"), Set())
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertTrue(stillOld.get.stringField.isEmpty)
        assertEquals(4, FooWithOptionalField.sync.count())
        assertEquals(old.get._id, stillOld.get._id)
    }

    @Test
    def testFindAndReplaceIgnoresId() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val old = Foo.sync[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one
        val stillOld = Foo.sync[Foo].findAndReplace(BObject("intField" -> 3),
            // this ObjectId() is the thing that needs to get ignored
            Foo(ObjectId(), 42, "42"))
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertEquals(4, Foo.sync.count())
        assertEquals(old.get._id, stillOld.get._id)

        // but we wrote out the new one and it has the old id
        val replaced = Foo.sync[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(42, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)
    }

    @Test
    def testFindAndReplaceNonexistent() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val notThere = Foo.sync[Foo].findAndReplace(BObject("intField" -> 124334),
            Foo(ObjectId(), 42, "42"))
        assertTrue(notThere.isEmpty)
    }

    @Test
    def testFindAndModify() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val old = Foo.sync[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we modify and return the old one
        val stillOld = Foo.sync[Foo].findAndModify(BObject("intField" -> 3),
            BObject("$inc" -> BObject("intField" -> 87)))
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertEquals(4, Foo.sync.count())
        assertEquals(old.get._id, stillOld.get._id)

        // but we wrote out the new one
        val replaced = Foo.sync[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(3 + 87, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)

        // now pass in the flag to return the new one and check that works
        val nowNew = Foo.sync[Foo].findAndModify(BObject("intField" -> 90),
            BObject("$inc" -> BObject("intField" -> 87)), Set[FindAndModifyFlag](FindAndModifyNew))
        assertTrue(nowNew.isDefined)
        assertEquals(3 + 87 + 87, nowNew.get.intField)
        assertEquals(old.get._id, nowNew.get._id)
    }

    @Test
    def testFindAndModifyWithSort() {
        create1234()
        assertEquals(4, Foo.sync.count())

        // in natural order, we're expecting 1, 2, 3, 4
        // since we insert them that way (not sure if mongo
        // guarantees this?)
        val firstNatural = Foo.sync[Foo].findOne()
        assertTrue(firstNatural.isDefined)
        assertEquals(1, firstNatural.get.intField)

        val sortedByIntBackward = Foo.sync[Foo].find(BObject("query" -> BObject(),
            "orderby" -> BObject("intField" -> -1)), AllFields, 0, 1, 0).toSeq
        assertTrue(!sortedByIntBackward.isEmpty)
        val last = sortedByIntBackward.last

        assertEquals(4, last.intField)

        // modify the last item sorting backward by intField
        val old = Foo.sync[Foo].findAndModify(BObject(),
            BObject("$inc" -> BObject("intField" -> 87)),
            BObject("intField" -> -1))

        assertTrue(old.isDefined)
        assertEquals(4, old.get.intField)
        assertEquals(4, Foo.sync.count())
        assertEquals(last._id, old.get._id)

        // check that the modification was made
        val replaced = Foo.sync[Foo].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(4 + 87, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)
    }

    @Test
    def testFindAndModifyWithFields() {
        create1234Optional()
        assertEquals(4, FooWithOptionalField.sync.count())

        val old = FooWithOptionalField.sync[FooWithOptionalField].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we replace and return the old one minus excluded field
        val stillOld = FooWithOptionalField.sync[FooWithOptionalField].findAndModify(BObject("intField" -> 3),
            BObject("$inc" -> BObject("intField" -> 87)), BObject(), ExcludedFields("stringField"), Set())
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertTrue(stillOld.get.stringField.isEmpty)
        assertEquals(4, FooWithOptionalField.sync.count())
        assertEquals(old.get._id, stillOld.get._id)

        // check that the modification was made
        val replaced = FooWithOptionalField.sync[FooWithOptionalField].findOneById(old.get._id)
        assertTrue(replaced.isDefined)
        assertEquals(90, replaced.get.intField)
        assertEquals(old.get._id, replaced.get._id)
    }

    @Test
    def testFindAndModifyNonexistent() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val notThere = Foo.sync[Foo].findAndModify(BObject("intField" -> 124334),
            BObject("$inc" -> BObject("intField" -> 87)))
        assertEquals(None, notThere)
    }

    @Test
    def testFindAndRemove() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val old = Foo.sync[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old.isDefined)
        assertEquals(3, old.get.intField)

        // check we remove and return the old one
        val stillOld = Foo.sync[Foo].findAndRemove(BObject("intField" -> 3))
        assertTrue(stillOld.isDefined)
        assertEquals(3, stillOld.get.intField)
        assertEquals(old.get._id, stillOld.get._id)

        // be sure it's removed
        assertEquals(3, Foo.sync.count())
        assertFalse(Foo.sync[Foo].findOneById(old.get._id).isDefined)
    }

    @Test
    def testFindAndRemoveWithSort() {
        create2143()
        assertEquals(4, Foo.sync.count())

        for (remaining <- 4 to 1) {
            assertEquals(remaining, Foo.sync.count())
            val removed = Foo.sync[Foo].findAndRemove(BObject(),
                BObject("intField" -> -1))
            assertTrue(removed.isDefined)
            assertEquals(remaining, removed.get.intField)
            assertEquals(remaining - 1, Foo.sync.count())
        }
    }

    @Test
    def testFindAndRemoveNonexistent() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val notThere = Foo.sync[Foo].findAndRemove(BObject("intField" -> 124334))
        assertTrue(notThere.isEmpty)
    }

    @Test
    def testInsert() {
        assertEquals(0, Foo.sync.count())
        val f = Foo(ObjectId(), 14, "14")
        Foo.sync[Foo].insert(f)
        assertEquals(1, Foo.sync.count())
        val foundById = Foo.sync[Foo].findOneById(f._id)
        assertTrue(foundById.isDefined)
        assertEquals(14, foundById.get.intField)

        // duplicate should throw
        val e = intercept[DuplicateKeyMongoException] {
            Foo.sync[Foo].insert(f)
        }
        assertTrue(e.getMessage.contains("duplicate key"))
        assertTrue(e.getMessage.contains("E11000"))
    }

    @Test
    def testSave() {
        assertEquals(0, Foo.sync.count())
        val f = Foo(ObjectId(), 14, "14")
        Foo.sync[Foo].save(f)
        assertEquals(1, Foo.sync.count())
        val foundById = Foo.sync[Foo].findOneById(f._id)
        assertTrue(foundById.isDefined)
        assertEquals(14, foundById.get.intField)

        // duplicate should not throw on a save, unlike insert
        Foo.sync[Foo].save(f)
        assertEquals(1, Foo.sync.count())
        val foundById2 = Foo.sync[Foo].findOneById(f._id)
        assertTrue(foundById2.isDefined)
        assertEquals(14, foundById2.get.intField)

        // making a change should be possible with a save
        val f2 = Foo(f._id, 15, "15")
        Foo.sync[Foo].save(f2)
        assertEquals(1, Foo.sync.count())
        val foundById3 = Foo.sync[Foo].findOneById(f2._id)
        assertTrue(foundById3.isDefined)
        assertEquals(15, foundById3.get.intField)
        assertEquals(f._id, foundById3.get._id)
    }

    @Test
    def testUpdate() {
        // updating when there's nothing there should do nothing
        assertEquals(0, Foo.sync.count())
        val f = Foo(ObjectId(), 14, "14")
        Foo.sync[Foo].update(BObject("_id" -> f._id), f)
        assertEquals(0, Foo.sync.count())
        val foundById = Foo.sync[Foo].findOneById(f._id)
        assertFalse(foundById.isDefined)

        // now use insert to add it
        assertEquals(0, Foo.sync.count())
        Foo.sync[Foo].insert(f)
        assertEquals(1, Foo.sync.count())

        // duplicate should not throw on an update
        Foo.sync[Foo].update(BObject("_id" -> f._id), f)
        assertEquals(1, Foo.sync.count())
        val foundById2 = Foo.sync[Foo].findOneById(f._id)
        assertTrue(foundById2.isDefined)
        assertEquals(14, foundById2.get.intField)

        // making a change should be possible with an update
        val f2 = Foo(f._id, 15, "15")
        Foo.sync[Foo].update(BObject("_id" -> f2._id), f2)
        assertEquals(1, Foo.sync.count())
        val foundById3 = Foo.sync[Foo].findOneById(f2._id)
        assertTrue(foundById3.isDefined)
        assertEquals(15, foundById3.get.intField)
        assertEquals(f._id, foundById3.get._id)
    }

    @Test
    def testUpdateWithModifier() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val old3 = Foo.sync[Foo].findOne(BObject("intField" -> 3))
        assertTrue(old3.isDefined)
        assertEquals(3, old3.get.intField)

        val old4 = Foo.sync[Foo].findOne(BObject("intField" -> 4))
        assertTrue(old4.isDefined)
        assertEquals(4, old4.get.intField)

        // update 3
        Foo.sync[Foo].updateWithModifier(BObject("intField" -> 3),
            BObject("$inc" -> BObject("intField" -> 87)))
        assertEquals(4, Foo.sync.count())

        // check we changed only one of the objects
        val replaced3 = Foo.sync[Foo].findOneById(old3.get._id)
        assertTrue(replaced3.isDefined)
        assertEquals(3 + 87, replaced3.get.intField)
        assertEquals(old3.get._id, replaced3.get._id)

        val replaced4 = Foo.sync[Foo].findOneById(old4.get._id)
        assertTrue(replaced4.isDefined)
        assertEquals(4, replaced4.get.intField)
        assertEquals(old4.get._id, replaced4.get._id)

        val replaced1 = Foo.sync[Foo].findOne(BObject("intField" -> 1))
        assertTrue(replaced1.isDefined)
        assertEquals(1, replaced1.get.intField)

        val replaced2 = Foo.sync[Foo].findOne(BObject("intField" -> 2))
        assertTrue(replaced2.isDefined)
        assertEquals(2, replaced2.get.intField)
    }

    @Test
    def testUpdateUpsert() {
        // upserting when there's nothing there should insert
        assertEquals(0, Foo.sync.count())
        val f = Foo(ObjectId(), 14, "14")
        Foo.sync[Foo].updateUpsert(BObject("_id" -> f._id), f)
        assertEquals(1, Foo.sync.count())
        val foundById = Foo.sync[Foo].findOneById(f._id)
        assertTrue("found by ID", foundById.isDefined)
        assertEquals(14, foundById.get.intField)

        // duplicate should not throw on an upsert
        Foo.sync[Foo].updateUpsert(BObject("_id" -> f._id), f)
        assertEquals(1, Foo.sync.count())
        val foundById2 = Foo.sync[Foo].findOneById(f._id)
        assertTrue("found by ID 2", foundById2.isDefined)
        assertEquals(14, foundById2.get.intField)

        // making a change should be possible with an upsert
        val f2 = Foo(f._id, 15, "15")
        Foo.sync[Foo].updateUpsert(BObject("_id" -> f2._id), f2)
        assertEquals(1, Foo.sync.count())
        val foundById3 = Foo.sync[Foo].findOneById(f2._id)
        assertTrue("found by ID 3", foundById3.isDefined)
        assertEquals(15, foundById3.get.intField)
        assertEquals(f._id, foundById3.get._id)
    }

    @Test
    def testUpdateUpsertWithModifier() {
        // Upsert using a modifier object, see
        // http://www.mongodb.org/display/DOCS/Updating

        assertEquals(0, Foo.sync.count())
        Foo.sync[Foo].updateWithModifier(BObject("intField" -> 57),
            BObject("$set" -> BObject("stringField" -> "hello")),
            UpdateOptions.upsert)
        assertEquals(1, Foo.sync.count())
        val found = Foo.sync[Foo].findOne()
        assertTrue(found.isDefined)
        assertEquals(57, found.get.intField)
        assertEquals("hello", found.get.stringField)
    }

    @Test
    def testUpdateMulti() {
        create1234()
        assertEquals(4, Foo.sync.count())

        assertEquals(10, Foo.sync[Foo].find().foldLeft(0)({ (v, f) => v + f.intField }))

        Foo.sync[Foo].updateMulti(BObject(), BObject("$inc" -> BObject("intField" -> 1)))

        val all = Foo.sync[Foo].find().toSeq
        assertEquals(4, all.length)

        val sum = all.foldLeft(0)({ (v, f) => v + f.intField })
        assertEquals(14, sum)
    }

    @Test
    def testRemove() {
        create1234()
        assertEquals(4, Foo.sync.count())

        Foo.sync[Foo].remove(BObject("intField" -> 3))
        assertEquals(3, Foo.sync.count())
        val all = Foo.sync[Foo].find().toSeq
        assertEquals(3, all.length)
        for (a <- all) {
            assertFalse(3 == a.intField)
        }
    }

    @Test
    def testRemoveById() {
        create1234()
        assertEquals(4, Foo.sync.count())

        val f = Foo.sync[Foo].findOne().get
        Foo.sync.removeById(f._id)
        assertEquals(3, Foo.sync.count())
        val all = Foo.sync[Foo].find().toSeq
        assertEquals(3, all.length)
        assertTrue(f._id == f._id) // be sure == at least kinda works on id
        for (a <- all) {
            assertFalse(f._id == a._id)
        }
    }

    @Test
    def testEnsureIndex() {
        val result = Foo.sync.ensureIndex(BObject("intField" -> 1))
        assertTrue(result.ok)
        val indexes = Foo.sync.findIndexes().toList
        val idIndex = indexes.find({ i => i.name == "_id_" })
        val intFieldIndex = indexes.find({ i => i.name == "intField_1" })
        assertTrue(idIndex.isDefined)
        assertTrue(intFieldIndex.isDefined)
        assertEquals(2, indexes.length)
        Foo.sync.dropIndex("intField_1")
        assertEquals(1, Foo.sync.findIndexes().length)
    }

    @Test
    def testEnsureIndexWithOptions() {
        val result = Foo.sync.ensureIndex(BObject("intField" -> 1),
            IndexOptions(Some("intFieldCustomName"), Set(IndexUnique, IndexBackground, IndexDropDups, IndexSparse), Some(1)))
        assertTrue(result.ok)
        val indexes = Foo.sync.findIndexes().toList
        val intFieldIndex = indexes.find({ i => i.name == "intFieldCustomName" }).get
        assertEquals(2, indexes.length)
        assertEquals(Foo.sync.fullName, intFieldIndex.ns)
        assertEquals(Some(true), intFieldIndex.unique)
        assertEquals(Some(true), intFieldIndex.background)
        assertEquals(Some(true), intFieldIndex.dropDups)
        assertEquals(Some(true), intFieldIndex.sparse)
        assertEquals(Some(1), intFieldIndex.v)
        Foo.sync.dropIndexes()
        assertEquals(1, Foo.sync.findIndexes().length)
    }

    @Test
    def testDropIndex() {
        val result = Foo.sync.ensureIndex(BObject("intField" -> 1))
        assertTrue(result.ok)
        val indexes = Foo.sync.findIndexes().toList
        assertEquals(2, indexes.length)
        Foo.sync.dropIndex("intField_1")
        assertEquals(1, Foo.sync.findIndexes().length)
    }

    @Test
    def testDropIndexes() {
        val result = Foo.sync.ensureIndex(BObject("intField" -> 1))
        assertTrue(result.ok)
        val result2 = Foo.sync.ensureIndex(BObject("stringField" -> -1))
        assertTrue(result2.ok)
        val indexes = Foo.sync.findIndexes().toList
        assertEquals(3, indexes.length)
        Foo.sync.dropIndexes()
        // the _id_ index doesn't drop so there's always 1
        assertEquals(1, Foo.sync.findIndexes().length)
    }

    private val objectManyTypes = BObjectTest.makeObjectManyTypes()
    private val arrayManyTypes = BObjectTest.makeArrayManyTypes()

    private def roundTrip[A <% BValue](value: A) {
        val bvalue: BValue = value

        // be sure wrapping in BValue is round-trippable or the
        // step involving MongoDB will never work!
        value match {
            case _: BValue =>
                assertEquals(value, bvalue)
            case _ =>
                assertEquals(value, bvalue.unwrapped)
        }

        val orig = BObject("_id" -> ObjectId(),
            "value" -> bvalue)
        val id = orig.getUnwrappedAs[ObjectId]("_id")
        Bar.sync.save(orig)
        val foundOption = Bar.sync.findOneById(id)
        assertTrue(foundOption.isDefined)
        assertEquals(id, foundOption.get.getUnwrappedAs[ObjectId]("_id"))
        assertEquals(orig, foundOption.get)

        // check that the round trip went all the way back to the
        // non-BValue that was passed in
        value match {
            case _: BValue =>
                assertEquals(value, foundOption.get.get("value").get)
            case _ =>
                assertEquals(value, foundOption.get.get("value").get.unwrapped)
        }

        Bar.sync.removeById(id)
    }

    @Test
    def testRoundTripString = roundTrip(BString("hello world"))

    @Test
    def testRoundTripMaxInt32 = roundTrip(BInt32(Int.MaxValue))

    @Test
    def testRoundTripMinInt32 = roundTrip(BInt32(Int.MinValue))

    @Test
    def testRoundTripObjectId = roundTrip(BObjectId(ObjectId()))

    @Test
    def testRoundTripNull = roundTrip(BNull)

    private val stringArray = BArray("hello", "world", "!")
    require(stringArray.size == 3)

    @Test
    def testRoundTripObjectWithArrayOfString = roundTrip(BObject("stringarray" -> stringArray))

    @Test
    def testRoundTripArrayOfString = roundTrip(stringArray)

    @Test
    def testRoundTripSeqOfString = roundTrip(Seq("a", "b", "c", "d"))

    @Test
    def testRoundTripMap = roundTrip(Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))

    @Test
    def testRoundTripManyTypes {
        for (kv <- objectManyTypes.iterator) {
            roundTrip(kv._2)
        }
    }

    @Test
    def testRoundTripMegaObject = roundTrip(objectManyTypes)

    @Test
    def testRoundTripMegaArray = roundTrip(arrayManyTypes)

    @Test
    def testCustomQueryReturnsVariousEntityTypes() {
        val foo = Foo(ObjectId(), 23, "woohoo")
        Foo.sync[Foo].save(foo)

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
    def testCustomQueryReturnsVariousEntityTypesWithIntId() {
        val foo = FooWithIntId(100, 23, "woohoo")
        FooWithIntId.sync[FooWithIntId].save(foo)

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
}
