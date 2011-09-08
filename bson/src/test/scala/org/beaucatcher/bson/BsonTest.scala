package org.beaucatcher.bson

import java.text.SimpleDateFormat
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import java.util.Date
import org.joda.time.{ DateTimeZone, DateTime }
import org.junit.Assert._
import org.junit._

class BsonTest extends TestUtils {

    @org.junit.Before
    def setup() {
    }

    val someDateTime = new DateTime(1996, 7, 10, 16, 50, 00, 00, DateTimeZone.UTC)
    val someJavaDate = someDateTime.toDate()

    protected def makeObjectManyTypes() = {
        BObject("null" -> null,
            "int" -> 42,
            "long" -> 37L,
            "bigint" -> BigInt(42),
            "double" -> 3.14159,
            "float" -> 3.14159f,
            "bigdecimal" -> BigDecimal(23.49),
            "boolean" -> true,
            "string" -> "quick brown fox",
            "date" -> someJavaDate,
            "datetime" -> someDateTime,
            "timestamp" -> new Timestamp((someJavaDate.getTime / 1000).toInt, 1),
            "objectid" -> new ObjectId("4dbf8ea93364e3bd9745723c"),
            "binary" -> new Binary(new Array[Byte](10), BsonSubtype.GENERAL),
            "map_int" -> Map[String, Int]("a" -> 20, "b" -> 21),
            "map_date" -> Map[String, Date]("a" -> someJavaDate, "b" -> someJavaDate),
            "seq_string" -> List("a", "b", "c", "d"),
            "seq_int" -> List(1, 2, 3, 4),
            "bobj" -> BObject("foo" -> 6789, "bar" -> 4321))
    }

    protected def makeArrayManyTypes() = {
        // a non-homogeneous-typed array is pretty much nonsense, but JavaScript
        // lets you do whatever, so we let you do whatever.
        BArray(null,
            42,
            37L,
            BigInt(42),
            3.14159,
            3.14159f,
            BigDecimal(23.49),
            true,
            "quick brown fox",
            someJavaDate,
            someDateTime,
            Timestamp((someJavaDate.getTime / 1000).toInt, 1),
            ObjectId("4dbf8ea93364e3bd9745723c"),
            Binary(new Array[Byte](10), BsonSubtype.GENERAL),
            Map[String, Int]("a" -> 20, "b" -> 21),
            Map[String, Date]("a" -> someJavaDate, "b" -> someJavaDate),
            List("a", "b", "c", "d"),
            List(1, 2, 3, 4),
            BObject("foo" -> 6789, "bar" -> 4321))
    }

    @Test
    def numericValuesBasicallyBehave() : Unit = {
        assertTrue(BInt32(1).isWhole)
        assertTrue(BInt64(1).isWhole)
        assertTrue(BDouble(1.0).isWhole)
        assertFalse(BDouble(1.5).isWhole)
        assertEquals(1, BDouble(1.0).intValue)
        assertTrue(math.abs(1.0 - BDouble(1.0).doubleValue) < 1e-6)

        assertEquals(BInt32(1), BInt32(1))
        assertEquals(BInt32(1), BInt64(1))
        assertEquals(BInt32(1), BDouble(1.0))
        assertEquals(BInt32(1), 1)
        assertTrue(1 == BInt32(1))
        assertTrue(BInt32(1) == 1)

        // This doesn't work, nor does it for Scala's BigInt.
        // but == does work. I see why this shouldn't work but
        // I don't see what makes == work.
        // assertEquals(1, BInt32(1))

        assertFalse(BInt32(1) == BInt32(2))
        assertFalse(BInt32(1) == BInt64(2))
        assertFalse(BInt32(1) == BDouble(2.0))
        assertFalse(BInt32(1) == 2)
        assertFalse(2 == BInt32(1))
        assertFalse(BInt32(1) == 2)
        assertFalse(BInt32(1).equals("1"))

        assertEquals(BInt32(1).hashCode, BInt32(1).hashCode)
        assertEquals(BInt32(1).hashCode, BInt64(1).hashCode)
        assertEquals(BInt32(1).hashCode, BDouble(1.0).hashCode)
        assertEquals(BInt32(1).hashCode, 1.##)

        assertFalse(BInt32(1).hashCode == BInt32(2).hashCode)
        assertFalse(BInt32(1).hashCode == BInt64(2).hashCode)
        assertFalse(BInt32(1).hashCode == BDouble(2.0).hashCode)
        assertFalse(BInt32(1).hashCode == (2.##))

        assertFalse(BInt32(2).hashCode == BDouble(1.5).hashCode)
        assertTrue(BInt32(2).hashCode == BDouble(2.0).hashCode)
        assertFalse(BInt32(2).hashCode == BDouble(2.5).hashCode)
        assertFalse(BInt32(2).hashCode == BDouble(3.0).hashCode)
    }

    @Test
    def numericValuesEqualPrimitives() : Unit = {
        assertEquals(BInt32(1), 1 : Char)
        assertEquals(BInt32(1), 1 : Byte)
        assertEquals(BInt32(1), 1 : Short)
        assertEquals(BInt32(1), 1 : Int)
        assertEquals(BInt32(1), 1 : Long)
        assertEquals(BInt32(1), 1 : Float)
        assertEquals(BInt32(1), 1 : Double)

        assertEquals(BDouble(1), 1 : Char)
        assertEquals(BDouble(1), 1 : Byte)
        assertEquals(BDouble(1), 1 : Short)
        assertEquals(BDouble(1), 1 : Int)
        assertEquals(BDouble(1), 1 : Long)
        assertEquals(BDouble(1), 1 : Float)
        assertEquals(BDouble(1), 1 : Double)
    }

    @Test
    def numericValueConversions() : Unit = {
        for (n <- List(BInt32(5), BInt64(5), BDouble(5))) {
            assertEquals(5, n.intValue)
            assertEquals(5, n.longValue)
            assertTrue(math.abs(5 - n.floatValue) < 1e-6)
            assertTrue(math.abs(5 - n.doubleValue) < 1e-6)
        }
    }

    // We have a bunch of apply() overloads to handle different numbers of args,
    // here we be sure they're all going to compile with and without implicit
    // conversion
    @Test
    def objectAppliesWork() : Unit = {
        // 0
        assertEquals(0, BObject().size)
        assertEquals(0, BObject.empty.size)

        // 1
        assertEquals(1, BObject("a" -> 1).size)
        assertEquals(1, BObject("a" -> BInt32(1)).size)

        // 2
        assertEquals(2, BObject("a" -> 1, "b" -> 2).size)
        assertEquals(2, BObject("a" -> BInt32(1), "b" -> BInt32(2)).size)

        // 2 heterogeneous
        assertEquals(2, BObject("a" -> 1, "b" -> "foo").size)

        // 3
        assertEquals(3, BObject("a" -> 1, "b" -> 2, "c" -> 3).size)
        assertEquals(3, BObject("a" -> BInt32(1), "b" -> BInt32(2), "c" -> BInt32(3)).size)

        // list
        assertEquals(2, BObject(List[(String, BValue)](Pair("a", BInt32(1)), Pair("b", BInt32(2)))).size)

        // 0
        assertEquals(0, JObject().size)
        assertEquals(0, JObject.empty.size)

        // 1
        assertEquals(1, JObject("a" -> 1).size)
        assertEquals(1, JObject("a" -> BInt32(1)).size)

        // 2
        assertEquals(2, JObject("a" -> 1, "b" -> 2).size)
        assertEquals(2, JObject("a" -> BInt32(1), "b" -> BInt32(2)).size)

        // 2 heterogeneous
        assertEquals(2, JObject("a" -> 1, "b" -> "foo").size)

        // 3
        assertEquals(3, JObject("a" -> 1, "b" -> 2, "c" -> 3).size)
        assertEquals(3, JObject("a" -> BInt32(1), "b" -> BInt32(2), "c" -> BInt32(3)).size)

        // list
        assertEquals(2, JObject(List[(String, JValue)](Pair("a", BInt32(1)), Pair("b", BInt32(2)))).size)

        // with nulls (null is special-cased in the code so this is needed for coverage)
        assertEquals(BObject("a" -> BNull),
            BObject("a" -> null))
        assertEquals(BObject("a" -> BNull, "b" -> BNull),
            BObject("a" -> null, "b" -> null))
        assertEquals(BObject("a" -> BNull, "b" -> BNull, "c" -> BNull),
            BObject("a" -> null, "b" -> null, "c" -> null))
        assertEquals(BObject("a" -> BNull, "b" -> BNull, "c" -> BNull, "d" -> BNull),
            BObject("a" -> null, "b" -> null, "c" -> null, "d" -> null))
        assertEquals(BObject("a" -> BNull),
            BObject(Map[String, String]("a" -> null)))
        assertEquals(BObject("a" -> BNull),
            BObject(Pair("a", null)))
        assertEquals(JObject("a" -> BNull),
            JObject("a" -> null))
        assertEquals(JObject("a" -> BNull, "b" -> BNull),
            JObject("a" -> null, "b" -> null))
        assertEquals(JObject("a" -> BNull, "b" -> BNull, "c" -> BNull),
            JObject("a" -> null, "b" -> null, "c" -> null))
        assertEquals(JObject("a" -> BNull, "b" -> BNull, "c" -> BNull, "d" -> BNull),
            JObject("a" -> null, "b" -> null, "c" -> null, "d" -> null))
        assertEquals(JObject("a" -> BNull),
            JObject(Map[String, String]("a" -> null)))
        assertEquals(JObject("a" -> BNull),
            JObject(Pair("a", null)))
    }

    @Test
    def listAppliesWork() : Unit = {
        // 0
        assertEquals(0, BArray().size)
        assertEquals(0, BArray.empty.size)

        // 1
        assertEquals(1, BArray(1).size)
        assertEquals(1, BArray(BInt32(1)).size)

        // 2
        assertEquals(2, BArray(1, 2).size)
        assertEquals(2, BArray(BInt32(1), BInt32(2)).size)

        // 2 heterogeneous
        assertEquals(2, BArray(1, "foo").size)

        // 3
        assertEquals(3, BArray(1, 2, 3).size)
        assertEquals(3, BArray(BInt32(1), BInt32(2), BInt32(3)).size)

        // list
        assertEquals(3, BArray(List(1, 2, 3)).size)
        assertEquals(3, BArray(List(BInt32(1), BInt32(2), BInt32(3))).size)

        // array of arrays
        assertEquals(1, BArray(BArray(1, 2, 3) : BValue).size)

        // 0
        assertEquals(0, JArray().size)
        assertEquals(0, JArray.empty.size)

        // 1
        assertEquals(1, JArray(1).size)
        assertEquals(1, JArray(BInt32(1)).size)

        // 2
        assertEquals(2, JArray(1, 2).size)
        assertEquals(2, JArray(BInt32(1), BInt32(2)).size)

        // 2 heterogeneous
        assertEquals(2, JArray(1, "foo").size)

        // 3
        assertEquals(3, JArray(1, 2, 3).size)
        assertEquals(3, JArray(BInt32(1), BInt32(2), BInt32(3)).size)

        // list
        assertEquals(3, JArray(List(1, 2, 3)).size)
        assertEquals(3, JArray(List(BInt32(1), BInt32(2), BInt32(3))).size)

        // array of arrays
        assertEquals(1, JArray(JArray(1, 2, 3) : JValue).size)

        // with nulls (the various constructors special-case null)
        assertEquals(BArray(BNull), BArray(null : BValue))
        assertEquals(BArray(BNull, BNull), BArray(null, null))
        assertEquals(BArray(BNull, BNull, BNull), BArray(null, null, null))
        assertEquals(BArray(BNull, BNull, BNull), BArray(Seq(null, null, null)))
        assertEquals(JArray(BNull), JArray(null : JValue))
        assertEquals(JArray(BNull, BNull), JArray(null, null))
        assertEquals(JArray(BNull, BNull, BNull), JArray(null, null, null))
        assertEquals(JArray(BNull, BNull, BNull), JArray(Seq(null, null, null)))

        // array indexing
        val a = BArray(0, 1, 2, 3, 4, 5)
        for (i <- 0 to 5) {
            assertEquals(BInt32(i), a(i))
        }
        val ja = JArray(0, 1, 2, 3, 4, 5)
        for (i <- 0 to 5) {
            assertEquals(BInt32(i), ja(i))
        }
    }

    @Test
    def buildBObject() = {
        // Test that we can use implicits on a bunch of different types to build a BObject
        // We're mostly testing that this compiles rather than that it runs...

        val bobj = makeObjectManyTypes()
        assertTrue("null becomes BNull", !bobj.iterator.contains(("null", null)))
    }

    @Test
    def buildBArray() = {
        // Test that we can use implicits on a bunch of different types to build a BObject
        // We're mostly testing that this compiles rather than that it runs...

        val barray = makeArrayManyTypes()
        assertTrue("null becomes BNull", !barray.contains(null))
    }

    @Test
    def bobjectToJson() = {
        val bobj = makeObjectManyTypes()
        val jsonString = bobj.toJson(JsonFlavor.CLEAN)
        // FIXME is the base64 encoding really supposed to have \r\n instead of a carriage return newline?
        val expected = "{\"null\":null,\"int\":42,\"long\":37,\"bigint\":42,\"double\":3.14159," +
            "\"float\":3.141590118408203,\"bigdecimal\":23.49,\"boolean\":true,\"string\":\"quick brown fox\"," +
            "\"date\":837017400000,\"datetime\":837017400000,\"timestamp\":837017400001,\"objectid\":\"4dbf8ea93364e3bd9745723c\"," +
            "\"binary\":\"AAAAAAAAAAAAAA==\\r\\n\",\"map_int\":{\"a\":20,\"b\":21}," +
            "\"map_date\":{\"a\":837017400000,\"b\":837017400000},\"seq_string\":[\"a\",\"b\",\"c\",\"d\"]," +
            "\"seq_int\":[1,2,3,4],\"bobj\":{\"foo\":6789,\"bar\":4321}}"
        assertEquals(expected, jsonString)

        // FIXME test pretty string, test other json flavors
    }

    @Test
    def bobjectGetUnwrappedAs() : Unit = {
        val bobj = makeObjectManyTypes()
        val i = bobj.getUnwrappedAs[Int]("int")
        assertEquals(42, i)
        val badKey = intercept[NoSuchElementException] {
            val ignored = bobj.getUnwrappedAs[Int]("notinthere")
        }
        assertTrue(badKey.getMessage.contains("not found"))
        val wrongValueType = intercept[ClassCastException] {
            val ignored = bobj.getUnwrappedAs[String]("int")
        }
        // sometimes ClassCastException appears to have a null message
        //assertTrue(wrongValueType.getMessage.contains("cannot be cast"))
    }

    @Test
    def bobjectParseJson() : Unit = {
        val bobj = makeObjectManyTypes()
        val jobj = bobj.toJValue()
        val jsonString = bobj.toJson()
        val parsed = JValue.parseJson(jsonString)

        assertTrue("parsed an object", parsed.isInstanceOf[JObject])
        for ((orig, parsed) <- jobj.iterator zip parsed.asInstanceOf[JObject].iterator) {
            //printf("%s\t\t%s\n", orig, parsed)
            assertEquals(orig, parsed)
        }

        assertEquals("Parsed JSON equals pre-parsed JValue", jobj, parsed)

        val parsedPretty = JValue.parseJson(bobj.toPrettyJson())
        assertEquals("Parsing pretty JSON gives same result as compact", jobj, parsedPretty)
    }

    @Test
    def barrayToJson() = {
        val barray = makeArrayManyTypes()
        val jsonString = barray.toJson(JsonFlavor.CLEAN)
        val expected = "[null,42,37,42,3.14159,3.141590118408203,23.49,true,\"quick brown fox\",837017400000,837017400000,837017400001,\"4dbf8ea93364e3bd9745723c\",\"AAAAAAAAAAAAAA==\\r\\n\",{\"a\":20,\"b\":21},{\"a\":837017400000,\"b\":837017400000},[\"a\",\"b\",\"c\",\"d\"],[1,2,3,4],{\"foo\":6789,\"bar\":4321}]"
        assertEquals(expected, jsonString)

        // FIXME test pretty string, test other json flavors
    }

    @Test
    def bobjectAsMap() = {
        val empty = BObject.empty
        assertTrue(empty.isEmpty)
        val one = BObject("a" -> 1)
        assertFalse(one.isEmpty)
        assertEquals(1, one.size)
        assertTrue(one.iterator.toList == List(("a", BInt32(1))))

        // check map concat merges keys
        val deduplicated = one ++ one
        assertEquals(1, deduplicated.size)

        // check "+"
        val two = one + ("b" -> BInt32(2))
        assertEquals(2, two.size)
        // since our map preserves order, this must work
        assertEquals(List(("a", BInt32(1)), ("b", BInt32(2))), two.iterator.toList)

        // check preserving order a different way
        val backward = BObject("b" -> 2) + ("a" -> BInt32(1))
        assertTrue(backward.iterator.toList == List(("b", BInt32(2)), ("a", BInt32(1))))

        // check "-"
        val minused = two - "a"
        assertEquals(1, one.size)
        assertEquals(List(("b", BInt32(2))), minused.iterator.toList)

        // rebuild the big object with map operators (you'd want to use a builder
        // in real life of course)
        val big = makeObjectManyTypes()
        var built = BObject.empty
        for { kv <- big }
            built = built + kv
        assertEquals(big.size, built.size)

        // manually compare (gives easier to debug errors)
        for {
            (kv1, kv2) <- big zip built
        } {
            //System.out.println(kv1 + " " + kv2)
            assertEquals(kv1, kv2)
        }

        // compare the whole thing
        assertEquals(big.toList, built.toList)
    }

    // Check that v == wrap(v.unwrapped) for all types
    @Test
    def wrapProducesSameResultAsImplicit() = {
        val obj = makeObjectManyTypes()
        for {
            (k, v) <- obj
        } {
            assertEquals(v, BValue.wrap(v.unwrapped))
        }
    }

    @Test
    def wrapInt64() = {
        assertEquals(BInt64(Long.MaxValue), BValue.wrap(Long.MaxValue))
    }

    @Test
    def wrapJavaDate() = {
        assertEquals(BISODate(new DateTime(someJavaDate)), BValue.wrap(someJavaDate))
    }

    @Test
    def wrapBValue() = {
        assertEquals(BInt32(56), BValue.wrap(BInt32(56)))
    }

    @Test
    def wrapNotSupported() = {
        val fail = intercept[UnsupportedOperationException] {
            BValue.wrap(new Object())
        }
        assertTrue(fail.getMessage.contains("convert to BValue"))
    }

    @Test
    def unwrapWorks() = {
        def recursivelyCheckNotABValue(o : Any) : Unit = {
            o match {
                case b : BValue =>
                    throw new Exception("Found a BValue! " + b)
                case t : Traversable[_] =>
                    t.foreach(recursivelyCheckNotABValue)
                case p : Product =>
                    p.productIterator.foreach(recursivelyCheckNotABValue)
                case _ =>
            }
        }

        val obj = makeObjectManyTypes()

        recursivelyCheckNotABValue(obj.unwrapped)
    }

    @Test
    def rewrapWorks() = {
        val obj = makeObjectManyTypes()
        val rewrapped = BValue.wrap(obj.unwrapped)
        assertEquals(obj, rewrapped)
    }

    private def assertJavaValue(j : AnyRef) : Unit = {
        if (j != null) {
            val klassName = j.getClass.getName
            //System.out.println(k + "=" + klassName)
            assertFalse("found a scala type in unwrappedAsJava " + klassName,
                klassName.startsWith("scala."))
            assertTrue("found unexpected type in unwrappedAsJava " + klassName,
                klassName.contains("java") ||
                    klassName == "org.joda.time.DateTime" ||
                    klassName == "scalaj.collection.s2j.MapWrapper" ||
                    klassName == "scalaj.collection.s2j.SeqWrapper" ||
                    klassName.startsWith("org.bson.types."))

            // recurse
            j match {
                case c : java.util.Collection[_] =>
                    val i = c.iterator()
                    while (i.hasNext())
                        assertJavaValue(i.next().asInstanceOf[AnyRef])
                case _ =>
            }
        }
    }

    // check that unwrappedAsJava behaves sensibly
    // FIXME re-enable in a package that has org.bson conversions
    //@Test
    def javaUnwrapWorks() = {
        val obj = makeObjectManyTypes()
        for {
            (k, v) <- obj
        } {
            val j = v.unwrappedAsJava
            assertJavaValue(j)
            assertEquals(v, BValue.wrap(j))
        }
    }

    private def assertIsScalaMap(m : AnyRef) = {
        assertTrue("is a scala immutable map type: " + m.getClass.getName,
            m.getClass.getName.startsWith("scala.collection.immutable."))
    }

    private def assertIsScalaSeq(m : AnyRef) = {
        assertTrue("is a scala immutable seq type: " + m.getClass.getName,
            m.getClass.getName.startsWith("scala.collection.immutable."))
    }

    @Test
    def bobjectInteroperatesWithMap() = {
        // a type that can't be stored in a BValue and thus can't go in a BObject
        case class UnwrappableThing(foo : Int)

        val obj1 = makeObjectManyTypes()
        val obj2 = makeObjectManyTypes()

        val anotherBObject = obj1 ++ obj2
        assertEquals(classOf[BObject].getName, anotherBObject.getClass.getName)

        // with Map[Any,Any] in both directions
        val supertypeMap : Map[Any, Any] = Map(42 -> "foo", "bar" -> UnwrappableThing(10))
        val anotherMap = obj1 ++ supertypeMap
        assertIsScalaMap(anotherMap)
        val anotherMap2 = supertypeMap ++ obj1
        assertIsScalaMap(anotherMap2)

        // with Map[String,BValue] in both directions (order changes the type we get)
        val compatibleMap : Map[String, BValue] = Map("foo" -> 42, "bar" -> "baz")
        val withCompat = obj1 ++ compatibleMap
        assertEquals(classOf[BObject].getName, withCompat.getClass.getName)
        val withCompat2 = compatibleMap ++ obj1
        assertIsScalaMap(withCompat2)

        // with Map[String,BInt32] in both directions (order changes the type we get)
        val subtypeMap : Map[String, BInt32] = Map("foo" -> 42, "bar" -> 37)
        val withSub = obj1 ++ subtypeMap
        assertEquals(classOf[BObject].getName, withSub.getClass.getName)
        val withSub2 = subtypeMap ++ obj1
        assertIsScalaMap(withSub2)

        // filter, drop, take returns same type
        val filtered = obj1 filter { kv => kv._1 != "int" }
        assertEquals(classOf[BObject].getName, filtered.getClass.getName)
        assertEquals(obj1.size - 1, filtered.size)
        val taken = obj1 take 1
        assertEquals(classOf[BObject].getName, taken.getClass.getName)
        assertEquals(1, taken.size)
        val dropped = obj1 drop 1
        assertEquals(classOf[BObject].getName, dropped.getClass.getName)
        assertEquals(obj1.size - 1, dropped.size)

        // map returns same type as long as it's possible
        val mappedToBInt = obj1 map { kv => (kv._1, BInt32(1000)) }
        assertEquals(classOf[BObject].getName, mappedToBInt.getClass.getName)
        val mappedToUnwrappable = obj1 map { kv => (kv._1, UnwrappableThing(1001)) }
        assertIsScalaMap(mappedToUnwrappable)

        // add a tuple with non-BValue, get back a plain map
        val withNonBValue = (obj1 : Map[String, BValue]) + Pair("bar", UnwrappableThing(2))
        assertIsScalaMap(withNonBValue)

        // add a tuple with a BValue-convertible value will not implicitly convert
        val withConvertible = obj1 + ("bar" -> 42)
        assertIsScalaMap(withConvertible)

        // add a tuple with a BValue get back a BObject
        val withBValue = obj1 + ("bar" -> BInt32(42))
        assertEquals(classOf[BObject].getName, withBValue.getClass.getName)
    }

    @Test
    def barrayInteroperatesWithSeq() = {
        // a type that can't be stored in a BValue and thus can't go in a BObject
        case class UnwrappableThing(foo : Int)

        val arr1 = makeArrayManyTypes()
        val arr2 = makeArrayManyTypes()

        // with Seq[Any] in both directions
        val supertypeList : Seq[Any] = List("foo", UnwrappableThing(10))
        val anotherList = arr1 ++ supertypeList
        assertIsScalaSeq(anotherList)
        val anotherList2 = supertypeList ++ arr1
        assertIsScalaSeq(anotherList2)

        // with Seq[BValue] in both directions (order changes the type we get)
        val compatibleSeq : Seq[BValue] = List(42, "baz")
        val withCompat = arr1 ++ compatibleSeq
        assertEquals(classOf[BArray].getName, withCompat.getClass.getName)
        val withCompat2 = compatibleSeq ++ arr1
        assertIsScalaSeq(withCompat2)

        val anotherBArray = arr1 ++ arr2
        assertEquals(classOf[BArray].getName, anotherBArray.getClass.getName)

        // with Seq[BInt32] in both directions (order changes the type we get)
        val subtypeSeq : Seq[BInt32] = List(42, 37)
        val withSub = arr1 ++ subtypeSeq
        assertEquals(classOf[BArray].getName, withSub.getClass.getName)
        val withSub2 = subtypeSeq ++ arr1
        assertIsScalaSeq(withSub2)

        // filter, drop, take returns same type
        val filtered = arr1 filter { v => v != BInt64(37) }
        assertEquals(classOf[BArray].getName, filtered.getClass.getName)
        assertEquals(arr1.size - 1, filtered.size)
        val taken = arr1 take 1
        assertEquals(classOf[BArray].getName, taken.getClass.getName)
        assertEquals(1, taken.size)
        val dropped = arr1 drop 1
        assertEquals(classOf[BArray].getName, dropped.getClass.getName)
        assertEquals(arr1.size - 1, dropped.size)

        // map returns same type as long as it's possible
        val mappedToBInt = arr1 map { v => BInt32(1000) }
        assertEquals(classOf[BArray].getName, mappedToBInt.getClass.getName)
        val mappedToUnwrappable = arr1 map { v => UnwrappableThing(1001) }
        assertIsScalaSeq(mappedToUnwrappable)

        // add an item with non-BValue, get back a plain seq
        val withNonBValue = arr1 ++ List(UnwrappableThing(2))
        assertIsScalaSeq(withNonBValue)

        // add an item with a BValue get back a BArray
        val withBValue = arr1 ++ List(BInt32(42))
        assertEquals(classOf[BArray].getName, withBValue.getClass.getName)
    }

    @Test
    def binDataEqualsWorks() : Unit = {
        val a1 = BBinary(Array[Byte](0, 1, 2, 3, 4, 127, -127, -1), BsonSubtype.GENERAL)
        val a2 = BBinary(Array[Byte](0, 1, 2, 3, 4, 127, -127, -1), BsonSubtype.GENERAL)
        val subDiffers = BBinary(Array[Byte](0, 1, 2, 3, 4, 127, -127, -1), BsonSubtype.UUID)
        val dataDiffers = BBinary(Array[Byte](2, 2, 1, 0, 1, 127, -127, -1), BsonSubtype.GENERAL)
        assertEquals("two identical BBinary are equal", a1, a2)
        assertFalse("BBinary with different subtype not equal", a1 == subDiffers)
        assertFalse("BBinary with different data not equal", a1 == dataDiffers)
        assertEquals("two identical BBinary have same hashCode", a1.hashCode, a2.hashCode)
        assertFalse("BBinary with different subtype have different hashCode", a1.hashCode == subDiffers.hashCode)
        assertFalse("BBinary with different data have different hashCode", a1.hashCode == dataDiffers.hashCode)
    }

    @Test
    def binaryToStringWorks() : Unit = {
        val short = BBinary(Array[Byte](0, 1, 2, 3, 4, 127, -127, -1), BsonSubtype.GENERAL)
        val longer = BBinary(Array[Byte](0, 1, 2, 3, 4, 127, -127, -1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), BsonSubtype.GENERAL)
        assertEquals("BBinary(Binary(00010203047f81ff@8,GENERAL))", short.toString)
        assertEquals("BBinary(Binary(00010203047f81ff0506...@19,GENERAL))", longer.toString)
    }

    @Test
    def getAsWorks() : Unit = {
        val obj = makeObjectManyTypes()

        def wrongType[T : Manifest](key : String) : Unit = {
            val e = intercept[ClassCastException] {
                val t = obj.getUnwrappedAs[T](key)
                println("Should have failed to unwrap value: " + t)
                println("Target type to get as was: " + manifest[T])
                t match {
                    case r : AnyRef =>
                        println("Type of value we should not have unwrapped is " + r.getClass.getName)
                    case _ =>
                        println("Value we should not have unwrapped is not an AnyRef")
                }
            }
        }

        def rightType[T : Manifest](key : String, expected : Any) : T = {
            val t = obj.getUnwrappedAs[T](key)
            assertEquals(expected, t)
            t
        }

        rightType[String]("null", null)
        rightType[AnyRef]("null", null)
        rightType[Any]("null", null)
        wrongType[Int]("null")
        wrongType[Boolean]("null")
        wrongType[Long]("null")
        wrongType[Double]("null")
        wrongType[AnyVal]("null")

        rightType[Int]("int", 42)
        rightType[java.lang.Integer]("int", 42)
        rightType[Any]("int", 42)
        wrongType[Long]("int")
        wrongType[String]("int")

        rightType[scala.Long]("long", 37L)
        rightType[java.lang.Long]("long", 37L)
        rightType[Any]("long", 37L)
        wrongType[String]("long")

        // a field created with BigInt is stored as an Int or Long,
        // not a BigInt. BValue canonicalizes the representation.
        rightType[Int]("bigint", 42)
        rightType[AnyRef]("bigint", 42)
        rightType[Any]("bigint", 42)
        wrongType[BigDecimal]("bigint")
        wrongType[BigInt]("bigint")

        rightType[Double]("double", 3.14159)
        rightType[java.lang.Double]("double", 3.14159)
        wrongType[Float]("double")
        wrongType[Int]("double")
        wrongType[BigDecimal]("double")

        // Floats are stored as doubles
        rightType[Double]("float", 3.14159f.asInstanceOf[Double])
        rightType[java.lang.Double]("float", 3.14159f.asInstanceOf[Double])
        wrongType[Float]("float")
        wrongType[java.lang.Float]("float")
        wrongType[Int]("float")
        wrongType[BigDecimal]("float")

        // a field created with BigDecimal is stored as a double,
        // BValue canonicalizes the representation.
        rightType[Double]("bigdecimal", BigDecimal(23.49).toDouble)
        rightType[AnyRef]("bigdecimal", BigDecimal(23.49).toDouble)
        wrongType[BigDecimal]("bigdecimal")

        rightType[Boolean]("boolean", true)
        rightType[java.lang.Boolean]("boolean", true)
        wrongType[Int]("boolean")

        rightType[String]("string", "quick brown fox")
        wrongType[Int]("string")

        // java Date becomes a DateTime
        // FIXME there's some time zone problem or something here
        //rightType[DateTime]("date", someDateTime)
        wrongType[Date]("date")
        wrongType[Long]("date")

        // FIXME there's some time zone problem or something here
        //rightType[DateTime]("datetime", someDateTime)
        wrongType[Date]("datetime")
        wrongType[Long]("datetime")

        rightType[Timestamp]("timestamp", Timestamp((someJavaDate.getTime / 1000).toInt, 1))
        wrongType[DateTime]("timestamp")
        wrongType[Long]("timestamp")

        rightType[ObjectId]("objectid", new ObjectId("4dbf8ea93364e3bd9745723c"))
        rightType[AnyRef]("objectid", new ObjectId("4dbf8ea93364e3bd9745723c"))
        wrongType[String]("objectid")

        // FIXME maybe equality on Binary isn't by value?
        //rightType[Binary]("binary", new Binary(BsonSubtype.toByte(BsonSubtype.GENERAL), new Array[Byte](10)))
        wrongType[String]("binary")

        // FIXME add these
        // "map_int"
        // "map_date"
        // "seq_string"
        // "seq_int"
        // "bobj"
    }
}
