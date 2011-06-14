import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.junit.Assert._
import org.junit._
import net.liftweb.{ json => lift }
import java.io.Reader
import scala.io.Source

class JsonTest extends TestUtils {

    @org.junit.Before
    def setup() {
    }

    @Test
    def tokenizeAllBasicTypes() : Unit = {
        import JsonToken._

        // empty string
        assertEquals(List(Start, End),
            tokenize("".iterator).toList)
        // all token types with no spaces (not sure JSON spec wants this to work,
        // but spec is unclear to me when spaces are required, and banning them
        // is actually extra work)
        assertEquals(List(Start, Comma, Colon, CloseObject, OpenObject, CloseArray, OpenArray, StringValue("foo"),
            LongValue(42), TrueValue, DoubleValue(3.14), FalseValue, NullValue, End),
            tokenize(""",:}{]["foo"42true3.14falsenull""".iterator).toList)
        // all token types with spaces
        assertEquals(List(Start, Comma, Colon, CloseObject, OpenObject, CloseArray, OpenArray, StringValue("foo"),
            LongValue(42), TrueValue, DoubleValue(3.14), FalseValue, NullValue, End),
            tokenize(""" , : } { ] [ "foo" 42 true 3.14 false null """.iterator).toList)
        // all token types with extra spaces
        assertEquals(List(Start, Comma, Colon, CloseObject, OpenObject, CloseArray, OpenArray, StringValue("foo"),
            LongValue(42), TrueValue, DoubleValue(3.14), FalseValue, NullValue, End),
            tokenize("""   ,   :   }   {   ]   [   "foo"   42   true   3.14   false   null   """.iterator).toList)
    }

    @Test
    def tokenizerUnescapeStrings() : Unit = {
        import JsonToken._

        case class UnescapeTest(escaped : String, result : StringValue)
        implicit def pair2unescapetest(pair : (String, StringValue)) : UnescapeTest = UnescapeTest(pair._1, pair._2)

        // getting the actual 6 chars we want in a string is a little pesky.
        // \u005C is backslash. Just prove we're doing it right here.
        assertEquals(6, "\\u0046".length)
        assertEquals('4', "\\u0046"(4))
        assertEquals('6', "\\u0046"(5))

        val tests = List[UnescapeTest]((""" "" """, StringValue("")),
            (" \"\0\" ", StringValue("\0")), // nul byte
            (""" "\"\\\/\b\f\n\r\t" """, StringValue("\"\\/\b\f\n\r\t")),
            ("\"\\u0046\"", StringValue("F")),
            ("\"\\u0046\\u0046\"", StringValue("FF")))

        for (t <- tests) {
            assertEquals(List(Start, t.result, End),
                tokenize(t.escaped.iterator).toList)
        }

        def renderString(s : String) = {
            BString(s).toJson()
        }

        // check that we re-escape correctly too
        for (t <- tests) {
            val rendered = renderString(t.result.value)
            val reparsed = tokenize(rendered.iterator).toList match {
                case List(Start, StringValue(s), End) => s
                case _ => throw new AssertionError("Failed to reparse rendered string")
            }
            assertEquals(t.result.value, reparsed)
        }
    }

    @Test
    def tokenizerThrowsOnInvalidStrings() : Unit = {
        import JsonToken._

        val invalidTests = List(""" "\" """, // nothing after a backslash
            """ "\q" """, // there is no \q escape sequence
            "\"\\u123\"", // too short
            "\"\\u12\"", // too short
            "\"\\u1\"", // too short
            "\"\\u\"", // too short
            "\"", // just a single quote
            """ "abcdefg""" // no end quote
            )
        for (t <- invalidTests) {
            describeFailure(t) {
                intercept[JsonParseException] {
                    // note, toList is mandatory to actually traverse the tokens, which are
                    // lazily computed
                    tokenize(t.iterator).toList
                }
            }
        }
    }

    @Test
    def tokenizerParseNumbers() : Unit = {
        import JsonToken._

        abstract class NumberTest[+A](val s : String, val result : A)
        case class LongTest(override val s : String, override val result : LongValue) extends NumberTest[LongValue](s, result)
        case class DoubleTest(override val s : String, override val result : DoubleValue) extends NumberTest[DoubleValue](s, result)
        implicit def pair2inttest(pair : (String, Int)) = LongTest(pair._1, LongValue(pair._2))
        implicit def pair2longtest(pair : (String, Long)) = LongTest(pair._1, LongValue(pair._2))
        implicit def pair2doubletest(pair : (String, Double)) = DoubleTest(pair._1, DoubleValue(pair._2))

        val tests = List[NumberTest[Value]](("1", 1),
            ("1.2", 1.2),
            ("1e6", 1e6),
            ("1e-6", 1e-6),
            ("-1", -1),
            ("-1.2", -1.2))

        for (t <- tests) {
            assertEquals(List(Start, t.result, End),
                tokenize(t.s.iterator).toList)
        }
    }

    private[this] def toLift(value : JValue) : lift.JValue = {
        value match {
            case v : JObject =>
                lift.JObject(v.map({ kv => lift.JField(kv._1, toLift(kv._2)) }).toList)
            case v : JArray =>
                lift.JArray(v.value.map({ elem => toLift(elem) }))
            case v : BBoolean =>
                lift.JBool(v.value)
            case v : BInt32 =>
                lift.JInt(v.value)
            case v : BInt64 =>
                lift.JInt(v.value)
            case v : BDouble =>
                lift.JDouble(v.value)
            case v : BString =>
                lift.JString(v.value)
            case BNull =>
                lift.JNull
        }
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
        "\"", // single quote by itself
        "{ \"foo\" : }", // no value in object
        "{ : 10 }", // no key in object
        // these two problems are ignored by the lift tokenizer
        JsonTest(true, "[:\"foo\", \"bar\"]"), // colon in an array; lift doesn't throw (tokenizer erases it)
        JsonTest(true, "[\"foo\" : \"bar\"]"), // colon in an array another way, lift ignores (tokenizer erases it)
        "[ foo ]", // not a known token
        "[ t ]", // start of "true" but ends wrong
        "[ tx ]",
        "[ tr ]",
        "[ trx ]",
        "[ tru ]",
        "[ trux ]",
        "[ truex ]",
        "[ 10x ]", // number token with trailing junk
        "[ 10e3e3 ]", // two exponents
        "[ \"hello ]", // unterminated string
        JsonTest(true, "{ \"foo\" , true }"), // comma instead of colon, lift is fine with this
        JsonTest(true, "{ \"foo\" : true \"bar\" : false }"), // missing comma between fields, lift fine with this
        "[ 10, }]", // array with } as an element
        "[ 10, {]", // array with { as an element
        "{}x", // trailing invalid token after the root object
        "[]x", // trailing invalid token after the root array
        JsonTest(true, "{}{}"), // trailing token after the root object - lift OK with it
        "{}true", // trailing token after the root object
        JsonTest(true, "[]{}"), // trailing valid token after the root array
        "[]true", // trailing valid token after the root array
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
        """[{},{},{},{}]""",
        """[[[[[[]]]]]]""",
        """{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":42}}}}}}}}""",
        // this long one is mostly to test rendering
        """{ "foo" : { "bar" : "baz", "woo" : "w00t" }, "baz" : { "bar" : "baz", "woo" : [1,2,3,4], "w00t" : true, "a" : false, "b" : 3.14, "c" : null } }""",
        "{}")

    // For string quoting, check behavior of escaping a random character instead of one on the list;
    // lift-json seems to oddly treat that as a \ literal

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
                    JValue.parseJson(Source.fromString(invalid.test))
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
            val ourASTSource = addOffendingJsonToException("beaucatcher-source", valid.test) {
                JValue.parseJson(Source.fromString(valid.test))
            }
            if (valid.liftBehaviorUnexpected) {
                // ignore this for now
            } else {
                addOffendingJsonToException("beaucatcher", valid.test) {
                    assertEquals(ourAST, ourASTSource) // Source should be same as parsing the string
                }
                addOffendingJsonToException("beaucatcher", valid.test) {
                    assertEquals(liftASTReader, ourASTSource) // with Reader/Source, we still match Lift
                }
            }

            /*
            println("Original:")
            println(valid.test)
            println("Compact:")
            println(ourAST.toJson())
            println("Pretty:")
            println(ourAST.toPrettyJson())
            println("The End")
             */

            // for valid JSON, we should be able to re-serialize and reparse and get same thing back
            val roundtripped = JValue.parseJson(ourAST.toJson())
            val roundtrippedPretty = JValue.parseJson(ourAST.toPrettyJson())
            assertEquals(ourAST, roundtripped)
            assertEquals(ourAST, roundtrippedPretty)
        }
    }
}
