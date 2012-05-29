package org.beaucatcher.bobject

import org.beaucatcher.bson._
import org.junit.Assert._
import org.junit._
import Implicits._

class SelectorTest extends TestUtils {
    @Test
    def headAndTail(): Unit = {
        assertEquals(("", ""), Selector.headAndTail(""))
        assertEquals(("foo", ""), Selector.headAndTail("foo"))
        assertEquals(("/", "foo"), Selector.headAndTail("/foo"))
        assertEquals(("//", "foo"), Selector.headAndTail("//foo"))
        assertEquals(("//", "/foo"), Selector.headAndTail("///foo"))
        assertEquals(("//", "/"), Selector.headAndTail("///"))
        assertEquals(("//", ""), Selector.headAndTail("//"))
        assertEquals(("/", ""), Selector.headAndTail("/"))
        assertEquals(("foo", "/"), Selector.headAndTail("foo/"))
        assertEquals(("foo", "//"), Selector.headAndTail("foo//"))
        assertEquals(("foo", "///"), Selector.headAndTail("foo///"))
        assertEquals(("foo", "/bar"), Selector.headAndTail("foo/bar"))
    }

    @Test
    def splitSelector(): Unit = {
        val answers = Map(
            "" -> Nil,
            "foo" -> List("foo"),
            "/foo" -> List("/", "foo"),
            "//foo" -> List("//", "foo"),
            "/foo/" -> List("/", "foo", "/"),
            "foo/bar" -> List("foo", "/", "bar"),
            "///" -> List("//", "/"),
            "foo/bar/baz" -> List("foo", "/", "bar", "/", "baz"),
            "foo//bar//baz" -> List("foo", "//", "bar", "//", "baz"))
        for (kv <- answers) {
            assertEquals(kv._2, Selector.splitSelector(kv._1))
        }
    }

    @Test
    def selectChild(): Unit = {
        val obj = BObject("foo" -> BInt32(42))
        assertEquals(List(BInt32(42)), obj.select("foo"))
    }

    @Test
    def selectSelf(): Unit = {
        val obj = BObject("foo" -> BInt32(42))
        assertEquals(List(obj), obj.select("."))
    }

    @Test
    def selectOne(): Unit = {
        val obj1 = BObject("foo" -> BInt32(42))
        assertEquals(Some(BInt32(42)), obj1.selectOne("foo"))
        assertEquals(None, obj1.selectOne("bar"))

        val obj2 = BObject("foo" -> BInt32(42))
        assertEquals(Some(obj2), obj2.selectOne("."))
    }

    @Test
    def badSelectorsThrow(): Unit = {
        val obj = BObject("foo" -> BInt32(42))
        val badSelectors = Seq(
            "",
            "/",
            "//",
            "///",
            "/foo",
            "//foo",
            "///foo",
            "foo/",
            "foo//",
            "foo///",
            "foo/bar/",
            "foo/bar//",
            "foo/bar///")

        for (bad <- badSelectors) {
            try {
                intercept[BadSelectorException] {
                    obj.select(bad)
                }
            } catch {
                case e: Throwable =>
                    println("Failure on selector: '" + bad + "'")
                    throw e
            }
        }
    }

    @Test
    def selectGrandchild(): Unit = {
        val grandchild = BInt32(42)
        val child = BObject("bar" -> grandchild)
        val obj = BObject("foo" -> child)
        assertEquals(List(grandchild), obj.select("foo/bar"))
        assertEquals(Nil, obj.select("bar"))
    }

    @Test
    def selectDescendant(): Unit = {
        val grandchild = BInt32(42)
        val child = BObject("bar" -> grandchild)
        val obj = BObject("foo" -> child)
        assertEquals(List(grandchild), obj.select(".//bar"))
        assertEquals(Nil, obj.select("./bar"))
    }

    @Test
    def selectDescendants(): Unit = {
        val grandchild = BInt32(42)
        val child = BObject("bar" -> grandchild)
        val obj = BObject("foo" -> child, "boo" -> child, "woo" -> child)
        assertEquals(List(grandchild, grandchild, grandchild), obj.select(".//bar"))
    }

    @Test
    def selectNested(): Unit = {
        val greatgrandchild = BInt32(42)
        val grandchild = BObject("foo" -> greatgrandchild)
        val child = BObject("foo" -> grandchild)
        val obj = BObject("foo" -> child)
        assertEquals(List(child, grandchild, greatgrandchild), obj.select(".//foo"))
    }

    @Test
    def selectNestedWithIntermediate(): Unit = {
        val greatgreatgreatgrandchild = BInt32(42)
        val greatgreatgrandchild = BObject("foo" -> greatgreatgreatgrandchild)
        val greatgrandchild = BObject("bar" -> greatgreatgrandchild)
        val grandchild = BObject("baz" -> greatgrandchild)
        val child = BObject("foo" -> grandchild)
        val obj = BObject("foo" -> child)
        assertEquals(List(child, grandchild, greatgreatgreatgrandchild), obj.select(".//foo"))
    }

    @Test
    def selectAsFiltersWrongTypes(): Unit = {
        val greatgrandchild = BInt32(42)
        val grandchild = BObject("foo" -> greatgrandchild)
        val child = BObject("foo" -> grandchild)
        val obj = BObject("foo" -> child)
        assertEquals(List(child, grandchild, greatgrandchild), obj.select(".//foo"))
        assertEquals(List(greatgrandchild.value), obj.selectAs[Int](".//foo"))
        assertEquals(Nil, obj.selectAs[String](".//foo"))
    }

    @Test
    def wildcardChildren(): Unit = {
        val obj = BObject("a" -> 1, "b" -> 2, "c" -> 3)
        assertEquals(List(BInt32(1), BInt32(2), BInt32(3)), obj.select("*"))
        assertEquals(List(BInt32(1), BInt32(2), BInt32(3)), obj.select("./*"))
        assertEquals(List(BInt32(1), BInt32(2), BInt32(3)), obj.select(".//*"))
    }

    @Test
    def wildcardDescendants(): Unit = {
        val obj = BObject("a" -> 1, "b" -> 2, "c" -> 3, "d" -> BObject("e" -> 5, "f" -> 6, "g" -> BObject("h" -> 8)))
        val childA = BInt32(1)
        val childB = BInt32(2)
        val childC = BInt32(3)
        val childD = BObject("e" -> 5, "f" -> 6, "g" -> BObject("h" -> 8))
        val childE = BInt32(5)
        val childF = BInt32(6)
        val childG = BObject("h" -> BInt32(8))
        val childH = BInt32(8)
        val allDescendants = List(childA, childB, childC, childD, childE,
            childF, childG, childH)
        assertEquals(allDescendants, obj.select(".//*"))
        assertEquals(List(childE, childF, childG, childH), obj.select("d//*"))
    }

    @Test
    def arrayIndexing(): Unit = {
        val arr = BArray(0, 1, 2, 3, 4, 5)
        assertEquals(List(BInt32(0)), arr.select("0"))
        assertEquals(List(BInt32(1)), arr.select("1"))
        assertEquals(List(BInt32(5)), arr.select("5"))
        assertEquals(Nil, arr.select("-1"))
        assertEquals(Nil, arr.select("6"))
    }

    @Test
    def allArrayChildren(): Unit = {
        val arr = BArray(0, 1, 2)
        assertEquals(List(BInt32(0), BInt32(1), BInt32(2)), arr.select("*"))
        assertEquals(List(BInt32(0), BInt32(1), BInt32(2)), arr.select("./*"))
        assertEquals(List(BInt32(0), BInt32(1), BInt32(2)), arr.select(".//*"))
    }

    @Test
    def allArrayGrandchildren(): Unit = {
        val arr = BArray(BArray(0, 1, 2), BArray(0, 1, 2))
        assertEquals(List(BInt32(0), BInt32(1), BInt32(2), BInt32(0), BInt32(1), BInt32(2)), arr.select("./*/*"))
    }

    @Test
    def allArrayElementsAt2(): Unit = {
        val arr = BArray(BArray(0, 1, 2), BArray(0, 1, 2))
        assertEquals(List(BInt32(2), BInt32(2)), arr.select("./*/2"))
        assertEquals(List(BInt32(2), BInt32(2)), arr.select(".//2"))
    }
}
