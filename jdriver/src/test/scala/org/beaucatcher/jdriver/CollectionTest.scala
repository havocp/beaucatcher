package org.beaucatcher.jdriver

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.jdriver._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._
import foo._
import com.mongodb.DBObject

package foo {
    case class Foo(_id : ObjectId, intField : Int, stringField : String) extends abstractfoo.AbstractFoo

    object Foo extends CollectionAccessWithCaseClass[Foo, ObjectId]
        with JavaDriverTestProvider {
        def customQuery[E](implicit chooser : SyncCollectionChooser[E, _]) = {
            sync[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithIntId(_id : Int, intField : Int, stringField : String) extends abstractfoo.AbstractFooWithIntId

    object FooWithIntId extends CollectionAccessWithCaseClass[FooWithIntId, Int]
        with JavaDriverTestProvider {
        def customQuery[E](implicit chooser : SyncCollectionChooser[E, _]) = {
            sync[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithOptionalField(_id : ObjectId, intField : Int, stringField : Option[String]) extends abstractfoo.AbstractFooWithOptionalField

    object FooWithOptionalField extends CollectionAccessWithCaseClass[FooWithOptionalField, ObjectId]
        with JavaDriverTestProvider {
    }

    object Bar extends CollectionAccessWithoutEntity[ObjectId]
        with JavaDriverTestProvider {

    }
}

class CollectionTest
    extends AbstractCollectionTest[Foo, FooWithIntId, FooWithOptionalField](Foo, FooWithIntId, FooWithOptionalField, Bar) {
    override def newFoo(_id : ObjectId, intField : Int, stringField : String) = Foo(_id, intField, stringField)
    override def newFooWithIntId(_id : Int, intField : Int, stringField : String) = FooWithIntId(_id, intField, stringField)
    override def newFooWithOptionalField(_id : ObjectId, intField : Int, stringField : Option[String]) = FooWithOptionalField(_id, intField, stringField)

    override def roundTripThroughJava(bvalue : BValue) {
        // be sure we can round-trip through Java
        import JavaConversions._
        val jvalue = bvalue.unwrappedAsJava
        val wrapped = wrapJavaAsBValue(jvalue)
        assertEquals(bvalue, wrapped)

        // and be sure the wrap/unwrap of BObject works
        import org.beaucatcher.jdriver.Implicits._
        bvalue match {
            case o : BObject =>
                val dbval : DBObject = new BObjectDBObject(o)
                val asBObject : BObject = dbval
                assertEquals(bvalue, asBObject)
            case _ =>
        }
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
