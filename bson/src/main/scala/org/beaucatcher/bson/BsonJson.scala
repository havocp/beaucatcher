package org.beaucatcher.bson

import java.io.Reader
import net.liftweb.{ json => lift }
import scala.collection.mutable
import scala.collection.mutable.Builder

/** An exception related to JSON parsing or generation. */
abstract class JsonException(message : String, cause : Throwable = null) extends Exception(message, cause)
/** An exception indicating that a JSON string was not well-formed or something went wrong while parsing. */
class JsonParseException(message : String, cause : Throwable = null) extends JsonException(message, cause)
/** An exception indicating that a JSON string was invalid (didn't conform to a schema, for example). */
class JsonValidationException(message : String, cause : Throwable = null) extends JsonException(message, cause)

private[bson] object BsonJson {
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

    def toJson(value : BValue, flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : String = {
        require(value != null)
        lift.Printer.compact(lift.render(toLift(value.toJValue(flavor))))
    }

    def toPrettyJson(value : BValue, flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : String = {
        require(value != null)
        lift.Printer.pretty(lift.render(toLift(value.toJValue(flavor))))
    }

    private def withLiftExceptionsConverted[T](block : => T) : T = {
        try {
            block
        } catch {
            case e : lift.JsonParser.ParseException =>
                throw new JsonParseException(e.getMessage(), e)
        }
    }

    private def parseString(json : String) : JValue = {
        withLiftExceptionsConverted(lift.JsonParser.parse[JValue](json, parser))
    }

    private def parseReader(json : Reader) : JValue = {
        withLiftExceptionsConverted(lift.JsonParser.parse[JValue](json, parser))
    }

    def fromJson(json : String) : JValue = parseString(json)

    def fromJson(json : Reader) : JValue = parseReader(json)

    private val parser = (p : lift.JsonParser.Parser) => {
        import lift.JsonParser._

        def parseValue(t : Token, tokens : Iterator[Token]) : JValue = {
            t match {
                case StringVal(x) => BString(x)
                case IntVal(x) =>
                    if (x.isValidInt)
                        BInt32(x.intValue)
                    else if (x > Long.MaxValue || x < Long.MinValue)
                        BDouble(x.doubleValue)
                    else
                        BInt64(x.longValue)
                case DoubleVal(x) => BDouble(x)
                case BoolVal(x) => BBoolean(x)
                case NullVal => BNull
                case OpenObj => parseObj(tokens)
                case OpenArr => parseArr(tokens)
                case whatever => throw new lift.JsonParser.ParseException("Invalid token for field value or array element: " + whatever, null)
            }
        }

        def parseFields(builder : Builder[(String, JValue), List[(String, JValue)]], tokens : Iterator[Token]) : Unit = {
            val seen = mutable.Set[String]()
            while (true) {
                tokens.next match {
                    case FieldStart(name) =>
                        if (seen.contains(name)) {
                            parseValue(tokens.next, tokens) // discard and ignore (right thing?)
                        } else {
                            seen += name
                            val jvalue = parseValue(tokens.next, tokens)
                            builder += Pair(name, jvalue)
                        }
                    case CloseObj => return
                    case whatever =>
                        throw new lift.JsonParser.ParseException("Invalid token, expected } or field name, got " + whatever, null)
                }
            }
        }

        def parseElements(builder : Builder[JValue, List[JValue]], tokens : Iterator[Token]) : Unit = {
            while (true) {
                tokens.next match {
                    case CloseArr => return
                    case t : Token => builder += parseValue(t, tokens)
                }
            }
        }

        def parseObj(tokens : Iterator[Token]) : JObject = {
            val builder = List.newBuilder[(String, JValue)]
            parseFields(builder, tokens)
            JObject(builder.result)
        }

        def parseArr(tokens : Iterator[Token]) : JArray = {
            val builder = List.newBuilder[JValue]
            parseElements(builder, tokens)
            JArray(builder.result)
        }

        val tokens = new Iterator[Token] {
            override def next = p.nextToken
            override def hasNext = true // white lie
        }
        val result = tokens.next match {
            case OpenObj => parseObj(tokens)
            case OpenArr => parseArr(tokens)
            case End => throw new lift.JsonParser.ParseException("empty JSON document", null)
            case whatever => throw new lift.JsonParser.ParseException("JSON document must have array or object at root, has: " + whatever, null)
        }
        tokens.next match {
            case End => result
            case whatever => throw new lift.JsonParser.ParseException("JSON document has extra stuff after first object or array: " + whatever, null)
        }
    }
}
