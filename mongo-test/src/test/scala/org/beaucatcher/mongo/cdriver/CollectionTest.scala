package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo.cdriver._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._
import foo._

package foo {
    import BObjectCodecs._

    // FIXME this stuff can move up to AbstractCollectionTest since it no
    // longer depends on the driver

    case class Foo(_id: ObjectId, intField: Int, stringField: String) extends abstractfoo.AbstractFoo

    object Foo extends CollectionAccessWithCaseClass[Foo, ObjectId] {
        def customQuery[E](implicit context: Context, chooser: SyncCollectionChooser[E, _]) = {
            sync[E].find(BObject("intField" -> 23))
        }

        override def migrate(implicit context: Context): Unit = {
            // we're mostly doing this just to test that migrate() gets called
            // and works
            sync.ensureIndex(BObject("intField" -> 1))
        }
    }

    case class FooWithIntId(_id: Int, intField: Int, stringField: String) extends abstractfoo.AbstractFooWithIntId

    object FooWithIntId extends CollectionAccessWithCaseClass[FooWithIntId, Int] {
        def customQuery[E](implicit context: Context, chooser: SyncCollectionChooser[E, _]) = {
            sync[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithOptionalField(_id: ObjectId, intField: Int, stringField: Option[String]) extends abstractfoo.AbstractFooWithOptionalField

    object FooWithOptionalField extends CollectionAccessWithCaseClass[FooWithOptionalField, ObjectId] {
    }

    object Bar extends CollectionAccessWithoutEntity[ObjectId] {

    }
}

class CollectionTest
    extends AbstractCollectionTest[Foo, FooWithIntId, FooWithOptionalField](Foo, FooWithIntId, FooWithOptionalField, Bar)
    with ChannelDriverTestContextProvider {
    override def newFoo(_id: ObjectId, intField: Int, stringField: String) = Foo(_id, intField, stringField)
    override def newFooWithIntId(_id: Int, intField: Int, stringField: String) = FooWithIntId(_id, intField, stringField)
    override def newFooWithOptionalField(_id: ObjectId, intField: Int, stringField: Option[String]) = FooWithOptionalField(_id, intField, stringField)

    @Test
    def usingExpectedDriver(): Unit = {
        assertTrue("expecting to use channel driver",
            implicitly[Context].driver.getClass.getSimpleName.contains("ChannelDriver"))
    }

    // factoring this up into AbstractCollectionTest is just too annoying
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

    // factoring this up into AbstractCollectionTest is just too annoying
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
