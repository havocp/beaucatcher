import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.junit.Assert._
import org.junit._
import net.liftweb.{ json => lift }
import java.io.Reader

class JsonTest extends TestUtils {

    @org.junit.Before
    def setup() {
    }

    private[this] def fromLift(liftValue : lift.JValue) : JValue = {
        liftValue match {
            case lift.JObject(fields) =>
                JObject(fields.map({ field => (field.name, fromLift(field.value)) }))
            case lift.JArray(values) =>
                JArray(values.map(fromLift(_)))
            case lift.JField(name, value) =>
                throw new IllegalStateException("either JField was a toplevel from lift-json or this function is buggy")
            case lift.JInt(i) =>
                if (i.isValidInt) BInt32(i.intValue) else BInt64(i.longValue)
            case lift.JBool(b) =>
                BBoolean(b)
            case lift.JDouble(d) =>
                BDouble(d)
            case lift.JString(s) =>
                BString(s)
            case lift.JNull =>
                BNull
            case lift.JNothing =>
                throw new IllegalStateException("not sure what to do with lift's JNothing here, shouldn't happen")
        }
    }

    private def withLiftExceptionsConverted[T](block : => T) : T = {
        try {
            block
        } catch {
            case e : lift.JsonParser.ParseException =>
                throw new JsonParseException(e.getMessage(), e)
        }
    }

    // parse a string using Lift's AST which we then mechanically convert
    // to our AST. We then test by ensuring we have the same results as
    // lift for a variety of JSON strings.
    private def fromJsonWithLiftParser(json : String) : JValue = {
        withLiftExceptionsConverted(fromLift(lift.JsonParser.parse(json)))
    }

    private def fromJsonWithLiftParser(json : Reader) : JValue = {
        withLiftExceptionsConverted(fromLift(lift.JsonParser.parse(json)))
    }

    case class JsonTest(liftBehaviorUnexpected : Boolean, test : String)
    implicit def string2jsontest(test : String) : JsonTest = JsonTest(false, test)

    private val invalidJson = List[JsonTest]("", // empty document
        "{",
        "}",
        "[",
        "]",
        "10", // value not in array or object
        "\"foo\"", // value not in array or object
        "\"", // lift's tokenizer barfs on this so we can't handle it yet
        "{ \"foo\" : }", // no value in object
        "{ : 10 }", // no key in object
        // these two problems are ignored by the lift tokenizer so we can't throw on them either
        //JsonTest(true, "[:\"foo\", \"bar\"]"), // colon in an array; lift doesn't throw (tokenizer erases it)
        //JsonTest(true, "[\"foo\" : \"bar\"]"), // colon in an array another way, lift ignores (tokenizer erases it)
        "") // empty document again, just for clean formatting of this list ;-)

    // We'll automatically try each of these with whitespace modifications
    // so no need to add every possible whitespace variation
    private val validJson = List[JsonTest]("{}",
        "[]",
        """{ "foo" : "bar" }""",
        """["foo", "bar"]""",
        """{ "foo" : 42 }""",
        """[10, 11]""",
        """[10,"foo"]""",
        """{ "foo" : "bar", "baz" : "boo" }""",
        """{ "foo" : { "bar" : "baz" }, "baz" : "boo" }""",
        """{ "foo" : { "bar" : "baz", "woo" : "w00t" }, "baz" : "boo" }""",
        """{ "foo" : [10,11,12], "baz" : "boo" }""",
        JsonTest(true, """{ "foo" : "bar", "foo" : "bar2" }"""), // dup keys - lift just returns both, we use first
        "{}")

    private def whitespaceVariations(tests : Seq[JsonTest]) : Seq[JsonTest] = {
        val variations = List({ s : String => s }, // identity
            { s : String => " " + s },
            { s : String => s + " " },
            { s : String => " " + s + " " },
            { s : String => s.replace(" ", "") }, // this would break with whitespace in a key or value
            { s : String => s.replace(":", " : ") }, // could break with : in a key or value
            { s : String => s.replace(",", " , ") } // could break with , in a key or value
            )
        for {
            t <- tests
            v <- variations
        } yield JsonTest(t.liftBehaviorUnexpected, v(t.test))
    }

    private def addOffendingJsonToException[R](parserName : String, s : String)(body : => R) = {
        try {
            body
        } catch {
            case t : Throwable =>
                throw new AssertionError(parserName + " failed on '" + s + "'", t)
        }
    }

    @Test
    def invalidJsonThrows() : Unit = {
        // be sure Lift throws on the string
        for (invalid <- whitespaceVariations(invalidJson)) {
            if (invalid.liftBehaviorUnexpected) {
                // lift unexpectedly doesn't throw, confirm that
                fromJsonWithLiftParser(invalid.test)
                fromJsonWithLiftParser(new java.io.StringReader(invalid.test))
            } else {
                addOffendingJsonToException("lift", invalid.test) {
                    intercept[JsonParseException] {
                        fromJsonWithLiftParser(invalid.test)
                    }
                    intercept[JsonParseException] {
                        fromJsonWithLiftParser(new java.io.StringReader(invalid.test))
                    }
                }
            }
        }
        // be sure we also throw
        for (invalid <- whitespaceVariations(invalidJson)) {
            addOffendingJsonToException("beaucatcher", invalid.test) {
                intercept[JsonParseException] {
                    JValue.parseJson(invalid.test)
                }
                intercept[JsonParseException] {
                    JValue.parseJson(new java.io.StringReader(invalid.test))
                }
            }
        }
    }

    @Test
    def validJsonWorks() : Unit = {
        // be sure we do the same thing as Lift when we build our own
        // AST directly (the point is to avoid intermediate Lift AST objects)
        for (valid <- whitespaceVariations(validJson)) {
            val liftAST = addOffendingJsonToException("lift", valid.test) {
                fromJsonWithLiftParser(valid.test)
            }
            val ourAST = addOffendingJsonToException("beaucatcher", valid.test) {
                JValue.parseJson(valid.test)
            }
            if (valid.liftBehaviorUnexpected) {
                // ignore this for now
            } else {
                addOffendingJsonToException("beaucatcher", valid.test) {
                    assertEquals(liftAST, ourAST)
                }
            }

            val liftASTReader = addOffendingJsonToException("lift-reader", valid.test) {
                fromJsonWithLiftParser(new java.io.StringReader(valid.test))
            }
            val ourASTReader = addOffendingJsonToException("beaucatcher-reader", valid.test) {
                JValue.parseJson(new java.io.StringReader(valid.test))
            }
            if (valid.liftBehaviorUnexpected) {
                // ignore this for now
            } else {
                addOffendingJsonToException("beaucatcher", valid.test) {
                    assertEquals(ourAST, ourASTReader) // Reader should be same as parsing the string
                }
                addOffendingJsonToException("beaucatcher", valid.test) {
                    assertEquals(liftASTReader, ourASTReader) // with Reader, we still match Lift
                }
            }

            // for valid JSON, we should be able to re-serialize and reparse and get same thing back
            val roundtripped = JValue.parseJson(ourAST.toJson())
            assertEquals(ourAST, roundtripped)
        }
    }
}
