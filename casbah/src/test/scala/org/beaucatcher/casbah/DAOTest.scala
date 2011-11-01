package org.beaucatcher.casbah

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.casbah._
import org.beaucatcher.mongo._
import org.junit.Assert._
import org.junit._
import foo._
import com.mongodb.DBObject

package foo {
    case class Foo(_id : ObjectId, intField : Int, stringField : String) extends abstractfoo.AbstractFoo

    object Foo extends CollectionOperationsWithCaseClass[Foo, ObjectId]
        with CasbahTestProvider {
        def customQuery[E](implicit chooser : SyncDAOChooser[E, _]) = {
            syncDAO[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithIntId(_id : Int, intField : Int, stringField : String) extends abstractfoo.AbstractFooWithIntId

    object FooWithIntId extends CollectionOperationsWithCaseClass[FooWithIntId, Int]
        with CasbahTestProvider {
        def customQuery[E](implicit chooser : SyncDAOChooser[E, _]) = {
            syncDAO[E].find(BObject("intField" -> 23))
        }
    }

    case class FooWithOptionalField(_id : ObjectId, intField : Int, stringField : Option[String]) extends abstractfoo.AbstractFooWithOptionalField

    object FooWithOptionalField extends CollectionOperationsWithCaseClass[FooWithOptionalField, ObjectId]
        with CasbahTestProvider {
    }

    object Bar extends CollectionOperationsWithoutEntity[ObjectId]
        with CasbahTestProvider {

    }
}

class DAOTest
    extends AbstractDAOTest[Foo, FooWithIntId, FooWithOptionalField](Foo, FooWithIntId, FooWithOptionalField, Bar) {
    override def newFoo(_id : ObjectId, intField : Int, stringField : String) = Foo(_id, intField, stringField)
    override def newFooWithIntId(_id : Int, intField : Int, stringField : String) = FooWithIntId(_id, intField, stringField)
    override def newFooWithOptionalField(_id : ObjectId, intField : Int, stringField : Option[String]) = FooWithOptionalField(_id, intField, stringField)

    override def roundTripThroughJava(bvalue : BValue) {
        // be sure we can round-trip through Java
        import j.JavaConversions._
        val jvalue = bvalue.unwrappedAsJava
        val wrapped = wrapJavaAsBValue(jvalue)
        assertEquals(bvalue, wrapped)

        // and be sure the wrap/unwrap of BObject works
        import org.beaucatcher.casbah.Implicits._
        bvalue match {
            case o : BObject =>
                val dbval : DBObject = new BObjectDBObject(o)
                val asBObject : BObject = dbval
                assertEquals(bvalue, asBObject)
            case _ =>
        }
    }

    // factoring this up into AbstractDAOTest is just too annoying
    @Test
    def testCustomQueryReturnsVariousEntityTypes() {
        val foo = Foo(ObjectId(), 23, "woohoo")
        Foo.syncDAO[Foo].save(foo)

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

    // factoring this up into AbstractDAOTest is just too annoying
    @Test
    def testCustomQueryReturnsVariousEntityTypesWithIntId() {
        val foo = FooWithIntId(100, 23, "woohoo")
        FooWithIntId.syncDAO[FooWithIntId].save(foo)

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
