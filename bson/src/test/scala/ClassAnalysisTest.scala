import scala.tools.scalap.scalax.rules.scalasig.TypeRefType
import com.ometer.ClassAnalysis
import org.junit.Assert._
import org.junit._
import play.test._

case class IntAndString(foo : Int, bar : String)

object IntAndString {
    def apply(foo : Int = 103) : IntAndString = {
        IntAndString(foo, "Hi")
    }
}

case class IntAndOptionalString(foo : Int, bar : Option[String])

class ClassAnalysisTest extends UnitTest {

    @org.junit.Before
    def setup() {
    }

    @Test
    def fieldAnalysis() = {
        val analysis = new ClassAnalysis(classOf[IntAndString])
        val fieldNames = analysis.fieldNamesIterator.toList
        assertEquals(List("foo", "bar"), fieldNames)

        val fieldNamesAndValues = analysis.fieldIterator(IntAndString(31, "woot")).toList
        assertEquals(List(("foo", 31), ("bar", "woot")), fieldNamesAndValues)

        val fieldTypes = analysis.fieldTypesIterator
        val typePaths = for (t <- fieldTypes)
            yield {
                t match {
                    case TypeRefType(prefix, symbol, typeArgs) =>
                        symbol.path
                    case _ =>
                        assertTrue("non-ref type field", false)
                }
            }

        assertEquals(List("scala.Int", "scala.Predef.String"), typePaths.toList)
    }

    @Test
    def fieldAnalysisWithOptional() = {
        val analysis = new ClassAnalysis(classOf[IntAndOptionalString])
        val fieldNames = analysis.fieldNamesIterator.toList
        assertEquals(List("foo", "bar"), fieldNames)

        val fieldNamesAndValues = analysis.fieldIterator(IntAndOptionalString(31, Some("woot"))).toList
        assertEquals(List(("foo", 31), ("bar", Some("woot"))), fieldNamesAndValues)
    }

    @Test
    def getFieldsFromCaseClass() = {
        val analysis = new ClassAnalysis(classOf[IntAndString])
        val map = analysis.asMap(IntAndString(42, "brown fox"))
        assertEquals(Map("foo" -> 42, "bar" -> "brown fox"), map)
    }

    @Test
    def getFieldsFromCaseClassWithOptional() = {
        val analysis = new ClassAnalysis(classOf[IntAndOptionalString])
        val c = IntAndOptionalString(42, Some("brown fox"))
        val map = analysis.asMap(c)
        // the option should be pulled off the string here
        assertEquals(Map("foo" -> 42, "bar" -> "brown fox"), map)
        assertEquals(2, map.size)

        // if the option is None, it should not be put in the map at all
        val cWithoutString = IntAndOptionalString(42, None)
        val mapWithoutString = analysis.asMap(cWithoutString)
        assertEquals(Map("foo" -> 42), mapWithoutString)
        assertEquals(1, mapWithoutString.size)
    }

    @Test
    def createCaseClassFromFields() = {
        val analysis = new ClassAnalysis(classOf[IntAndString])
        val o = analysis.fromMap(Map("foo" -> 42, "bar" -> "brown fox"))
        assertEquals(IntAndString(42, "brown fox"), o)
    }

    @Test
    def createCaseClassWithOptionalFromFields() = {
        val analysis = new ClassAnalysis(classOf[IntAndOptionalString])
        val o = analysis.fromMap(Map("foo" -> 42, "bar" -> "brown fox"))
        assertEquals(IntAndOptionalString(42, Some("brown fox")), o)

        // can omit optional fields from the map
        val oWithoutString = analysis.fromMap(Map("foo" -> 42))
        assertEquals(IntAndOptionalString(42, None), oWithoutString)
    }

    @Test
    def createCaseClassWithMissingFieldsShouldThrow() = {
        val analysis = new ClassAnalysis(classOf[IntAndString])
        var failure : Option[Throwable] = None
        try {
            analysis.fromMap(Map("bar" -> "Boo"))
        } catch {
            case e : Exception =>
                failure = Some(e)
            case _ =>
        }
        assertTrue(failure.isDefined)
        assertTrue(failure.get.getMessage().contains("requires value for"))
    }
}
