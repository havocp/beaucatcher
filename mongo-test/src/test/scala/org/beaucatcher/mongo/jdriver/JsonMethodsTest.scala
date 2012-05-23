package org.beaucatcher.mongo.jdriver

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.JsonMethods
import org.beaucatcher.bson.ClassAnalysis
import org.junit.Assert._
import org.junit._
import scala.util.Random

package restdemo {
    import BObjectCodecs._

    case class Foo(_id: ObjectId, aString: String, anInt: Int)

    object Foo
        extends CollectionAccessWithEntitiesBObjectOrCaseClassIdObjectId[Foo]
        with JsonMethods[Foo] {
        // the default collection name would conflict with the Foo
        // in CollectionTest since tests are run concurrently;
        // in apps you don't usually have to set this manually
        override val collectionName = "restfoo"

        override val jsonAnalysis = new ClassAnalysis(classOf[Foo])
        override def jsonSync(implicit context: Context): BoundSyncCollection[BObject, BObject, BObject, _, _] = sync[BObject]
        override def createQueryForAllObjects = BObject() // this would be dangerous in a non-test

        // This object inherits a complete Collection for BObject and for the Foo case class,
        // plus CRUD methods that accept/return JSON strings. In this file we're
        // testing the CRUD methods.
    }

    case class FooWithIntId(_id: Int, aString: String, anInt: Int)
    object FooWithIntId
        extends CollectionAccessWithEntitiesBObjectOrCaseClass[FooWithIntId, Int]
        with JsonMethods[FooWithIntId] {
        // the default collection name would conflict with the FooWithIntId
        // in CollectionTest since tests are run concurrently;
        // in apps you don't usually have to set this manually
        override val collectionName = "restfooWithIntId"

        override val jsonAnalysis = new ClassAnalysis(classOf[FooWithIntId])
        override def jsonSync(implicit context: Context): BoundSyncCollection[BObject, BObject, BObject, _, _] = sync[BObject]
        override def createQueryForAllObjects = BObject() // this would be dangerous in a non-test

        override def parseJValueIdFromPath(path: String): BInt32 = {
            try {
                path.toInt
            } catch {
                case e: NumberFormatException =>
                    throw new JsonValidationException("ID must be an integer, not %s".format(path), e)
            }
        }

        // don't do this in a real program, please.
        private var nextId = 1
        override def generateJValueId(): BInt32 = {
            nextId += 1
            nextId - 1
        }

        override def createQueryForObject(path: String) = {
            BObject("_id" -> BInt32(path.toInt))
        }
    }
}

class JsonMethodsTest
    extends JavaDriverTestContextProvider {
    import restdemo._

    protected implicit def context: Context = mongoContext

    @org.junit.Before
    def setup() {
        Foo.sync.remove(BObject())
        FooWithIntId.sync.remove(BObject())
    }

    @Test
    def putAndGetWorks(): Unit = {
        // create an object
        val createdJson = Foo.createJson("""{ "aString" : "hello", "anInt" : 76 }""")
        // parse what we created
        val bobjectCreated = Foo.parseJson(createdJson)
        assertEquals("hello", bobjectCreated.get("aString").get.unwrapped)
        assertEquals(76, bobjectCreated.get("anInt").get.unwrapped)
        val createdIdString = bobjectCreated.get("_id").get.unwrapped.toString

        // get the object
        val gotJsonOption = Foo.readJson(Some(createdIdString))
        assertTrue(gotJsonOption.isDefined)
        // parse what we got
        val bobjectGot = Foo.parseJson(gotJsonOption.get)
        assertEquals(createdIdString, bobjectGot.get("_id").get.unwrapped.toString)
        assertEquals(bobjectCreated, bobjectGot)
        assertEquals("hello", bobjectGot.get("aString").get.unwrapped)
        assertEquals(76, bobjectGot.get("anInt").get.unwrapped)

        // update the object with the ID in the path only, not in JSON
        val modifiedJson = Foo.updateJson(createdIdString, """{ "aString" : "hello world", "anInt" : 57 }""")
        val gotModifiedJsonOption = Foo.readJson(Some(createdIdString))
        val bobjectModified = Foo.parseJson(gotModifiedJsonOption.get)
        assertEquals(createdIdString, bobjectModified.get("_id").get.unwrapped.toString)
        assertEquals("hello world", bobjectModified.get("aString").get.unwrapped)
        assertEquals(57, bobjectModified.get("anInt").get.unwrapped)

        // update the object with redundant ID in the JSON
        val modifiedJson2 = Foo.updateJson(createdIdString, """{ "_id" : """" +
            createdIdString + """", "aString" : "hello world 2", "anInt" : 23 }""")
        val gotModifiedJsonOption2 = Foo.readJson(Some(createdIdString))
        val bobjectModified2 = Foo.parseJson(gotModifiedJsonOption2.get)
        assertEquals(createdIdString, bobjectModified2.get("_id").get.unwrapped.toString)
        assertEquals("hello world 2", bobjectModified2.get("aString").get.unwrapped)
        assertEquals(23, bobjectModified2.get("anInt").get.unwrapped)
    }

    @Test
    def putAndGetWorksWithIntId(): Unit = {
        // create an object
        val createdJson = FooWithIntId.createJson("""{ "aString" : "hello", "anInt" : 76 }""")
        // parse what we created
        val bobjectCreated = FooWithIntId.parseJson(createdJson)
        assertEquals("hello", bobjectCreated.get("aString").get.unwrapped)
        assertEquals(76, bobjectCreated.get("anInt").get.unwrapped)
        val createdIdString = bobjectCreated.get("_id").get.unwrapped.toString

        // get the object
        val gotJsonOption = FooWithIntId.readJson(Some(createdIdString))
        assertTrue(gotJsonOption.isDefined)
        // parse what we got
        val bobjectGot = FooWithIntId.parseJson(gotJsonOption.get)
        assertEquals(createdIdString, bobjectGot.get("_id").get.unwrapped.toString)
        assertEquals(bobjectCreated, bobjectGot)
        assertEquals("hello", bobjectGot.get("aString").get.unwrapped)
        assertEquals(76, bobjectGot.get("anInt").get.unwrapped)

        // update the object with the ID in the path only, not in JSON
        val modifiedJson = FooWithIntId.updateJson(createdIdString, """{ "aString" : "hello world", "anInt" : 57 }""")
        val gotModifiedJsonOption = FooWithIntId.readJson(Some(createdIdString))
        val bobjectModified = FooWithIntId.parseJson(gotModifiedJsonOption.get)
        assertEquals(createdIdString, bobjectModified.get("_id").get.unwrapped.toString)
        assertEquals("hello world", bobjectModified.get("aString").get.unwrapped)
        assertEquals(57, bobjectModified.get("anInt").get.unwrapped)

        // update the object with redundant ID in the JSON
        val modifiedJson2 = FooWithIntId.updateJson(createdIdString, """{ "_id" : """ +
            createdIdString + """, "aString" : "hello world 2", "anInt" : 23 }""")
        val gotModifiedJsonOption2 = FooWithIntId.readJson(Some(createdIdString))
        val bobjectModified2 = FooWithIntId.parseJson(gotModifiedJsonOption2.get)
        assertEquals(createdIdString, bobjectModified2.get("_id").get.unwrapped.toString)
        assertEquals("hello world 2", bobjectModified2.get("aString").get.unwrapped)
        assertEquals(23, bobjectModified2.get("anInt").get.unwrapped)
    }

    @Test
    def deleteWorks(): Unit = {
        // create an object
        val createdJson = Foo.createJson("""{ "aString" : "hello", "anInt" : 76 }""")
        // parse what we created
        val bobjectCreated = Foo.parseJson(createdJson)
        assertEquals("hello", bobjectCreated.get("aString").get.unwrapped)
        assertEquals(76, bobjectCreated.get("anInt").get.unwrapped)
        val createdIdString = bobjectCreated.get("_id").get.unwrapped.toString

        // get the object
        val gotJsonOption = Foo.readJson(Some(createdIdString))
        assertTrue(gotJsonOption.isDefined)
        // parse what we got
        val bobjectGot = Foo.parseJson(gotJsonOption.get)
        assertEquals(createdIdString, bobjectGot.get("_id").get.unwrapped.toString)
        assertEquals(bobjectCreated, bobjectGot)
        assertEquals("hello", bobjectGot.get("aString").get.unwrapped)
        assertEquals(76, bobjectGot.get("anInt").get.unwrapped)

        // delete the object
        Foo.deleteJson(createdIdString)

        // fail to get the object
        val gotAfterDeleteJsonOption = Foo.readJson(Some(createdIdString))
        assertFalse("object is gone", gotAfterDeleteJsonOption.isDefined)
    }

    @Test
    def readAllObjectsWorks(): Unit = {
        // create some objects
        Foo.createJson("""{ "aString" : "hello", "anInt" : 76 }""")
        Foo.createJson("""{ "aString" : "hello2", "anInt" : 77 }""")
        Foo.createJson("""{ "aString" : "hello3", "anInt" : 78 }""")

        // read all
        val allJsonOption = Foo.readJson(None)
        assertTrue(allJsonOption.isDefined)
        val objects = Foo.parseJsonArray(allJsonOption.get)
        assertEquals(3, objects.size)
        val strings = objects.map(_ match {
            case obj: BObject => obj.get("aString").get.unwrapped.asInstanceOf[String]
            case _ => throw new Exception("not an object")
        })
        assertEquals(List("hello", "hello2", "hello3"), strings.sorted.toList)
    }
}
