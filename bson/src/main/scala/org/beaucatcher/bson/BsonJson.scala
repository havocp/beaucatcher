/*
 * Copyright 2011 Havoc Pennington
 * derived in part from lift-json,
 * Copyright 2009-2010 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.beaucatcher.bson

import scala.collection.mutable
import scala.collection.mutable.Builder
import scala.io.Source

/** An exception related to JSON parsing or generation. */
abstract class JsonException(message : String, cause : Throwable = null) extends Exception(message, cause)
/** An exception indicating that a JSON string was not well-formed or something went wrong while parsing. */
class JsonParseException(message : String, cause : Throwable = null) extends JsonException(message, cause)
/** An exception indicating that a JSON string was invalid (didn't conform to a schema, for example). */
class JsonValidationException(message : String, cause : Throwable = null) extends JsonException(message, cause)

private[bson] object BsonJson {
    import JsonToken._

    private class RichScalaCompatLong(x: Long) {
        def isValidInt   = x.toLong == x.toInt
    }
    private implicit def long2RichScalaCompatLong(x: Long): RichScalaCompatLong = new RichScalaCompatLong(x)

    object JsonFormatting extends Enumeration {
        type JsonFormatting = Value
        val Compact, Pretty = Value
    }

    private def flattenStrings(i : Iterator[String]) : String = {
        val sb = new StringBuilder
        i.foreach(sb.append(_))
        sb.toString
    }

    def toJson(value : BValue, flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : String = {
        require(value != null)
        flattenStrings(render(value.toJValue(flavor), 0, JsonFormatting.Compact))
    }

    def toPrettyJson(value : BValue, flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : String = {
        require(value != null)
        flattenStrings(render(value.toJValue(flavor), 0, JsonFormatting.Pretty))
    }

    def fromJson(json : String) : JValue = parse(tokenize(json.iterator))

    def fromJson(json : Source) : JValue = parse(tokenize(json))

    private def parse(tokens : Iterator[JsonToken]) : JValue = {
        def parseValue(t : JsonToken, tokens : Iterator[JsonToken]) : JValue = {
            t match {
                case StringValue(x) => BString(x)
                case LongValue(x) =>
                    if (x.isValidInt)
                        BInt32(x.intValue)
                    else
                        BInt64(x.longValue)
                case DoubleValue(x) => BDouble(x)
                case TrueValue => BBoolean(true)
                case FalseValue => BBoolean(false)
                case NullValue => BNull
                case OpenObject => parseObj(tokens)
                case OpenArray => parseArr(tokens)
                case whatever => throw new JsonParseException("Invalid token for field value or array element: " + whatever)
            }
        }

        // this is just after the opening brace { for the object
        def parseFields(builder : Builder[(String, JValue), List[(String, JValue)]], tokens : Iterator[JsonToken]) : Unit = {
            val seen = mutable.Set[String]()
            while (true) {
                tokens.next match {
                    case StringValue(name) =>
                        tokens.next match {
                            case Colon => // OK
                            case whatever =>
                                throw new JsonParseException("Expected colon after field name, got: %s".format(whatever))
                        }
                        if (seen.contains(name)) {
                            parseValue(tokens.next, tokens) // discard and ignore (right thing?)
                        } else {
                            seen += name
                            val jvalue = parseValue(tokens.next, tokens)
                            builder += Pair(name, jvalue)
                        }
                    case CloseObject => return
                    case whatever =>
                        throw new JsonParseException("Expected } or field name, got %s".format(whatever))
                }
                tokens.next match {
                    case CloseObject => return
                    case Comma =>
                    case whatever =>
                        throw new JsonParseException("Expected } or comma, got %s".format(whatever))
                }
            }
        }

        // this is just after the opening bracket [ for the array
        def parseElements(builder : Builder[JValue, List[JValue]], tokens : Iterator[JsonToken]) : Unit = {
            // first element in the array
            tokens.next match {
                case CloseArray => return
                case t : Value => builder += parseValue(t, tokens)
                case OpenObject => builder += parseObj(tokens)
                case OpenArray => builder += parseArr(tokens)
                case whatever => throw new JsonParseException("JSON array should have had first element or ended with ], instead had %s".format(whatever))
            }
            while (true) {
                // here we are just after a value, before any comma
                tokens.next match {
                    case CloseArray => return
                    case Comma =>
                    case whatever => throw new JsonParseException("JSON array should have ended or had a comma, instead had %s".format(whatever))
                }
                // now we are just after a comma
                tokens.next match {
                    case t : Value => builder += parseValue(t, tokens)
                    case OpenObject => builder += parseObj(tokens)
                    case OpenArray => builder += parseArr(tokens)
                    case whatever => throw new JsonParseException("JSON array should have had a new element after comma, instead had %s".format(whatever))
                }
            }
        }

        def parseObj(tokens : Iterator[JsonToken]) : JObject = {
            val builder = List.newBuilder[(String, JValue)]
            parseFields(builder, tokens)
            JObject(builder.result)
        }

        def parseArr(tokens : Iterator[JsonToken]) : JArray = {
            val builder = List.newBuilder[JValue]
            parseElements(builder, tokens)
            JArray(builder.result)
        }

        tokens.next match {
            case Start =>
            // internal error if this happens, so don't use JsonParseException
            case whatever => throw new IllegalStateException("JSON document did not start with a Start token (bug in json parser)")
        }

        val result = tokens.next match {
            case OpenObject => parseObj(tokens)
            case OpenArray => parseArr(tokens)
            case End => throw new JsonParseException("empty JSON document")
            case whatever => throw new JsonParseException("JSON document must have array or object at root, has: " + whatever)
        }

        tokens.next match {
            case End => result
            case whatever => throw new JsonParseException("JSON document has extra stuff after first object or array: " + whatever)
        }
    }

    private def renderString(s : String) : String = {
        val sb = new StringBuilder
        sb.append('"')
        for (c <- s) {
            sb.append(c match {
                // switch from lift-json
                case '"' => "\\\""
                case '\\' => "\\\\"
                case '\b' => "\\b"
                case '\f' => "\\f"
                case '\n' => "\\n"
                case '\r' => "\\r"
                case '\t' => "\\t"
                case c if ((c >= '\u0000' && c < '\u001f') || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) => "\\u%04x".format(c : Int)
                case c => c
            })
        }
        sb.append('"')
        sb.toString
    }

    private def generateJoin[A](whatever : Iterator[A],
        create : (A) => Iterator[String],
        separator : String) : Iterator[String] = {
        var first = true
        whatever.foldLeft[Iterator[String]](Iterator.empty)({ (soFar, a) =>
            if (first) {
                first = false
                soFar ++ create(a)
            } else {
                soFar ++ Iterator.single(separator) ++ create(a)
            }
        })
    }

    private def indentation(indentLevel : Int) = {
        val indent = "    "
        Iterator.fill(indentLevel)(indent)
    }

    private def renderObject(o : JObject, indentLevel : Int, formatting : JsonFormatting.Value) : Iterator[String] = {
        if (formatting == JsonFormatting.Pretty) {
            val fields = generateJoin(o.iterator, { kv : (String, JValue) =>
                indentation(indentLevel + 1) ++ Iterator(renderString(kv._1), " : ") ++
                    render(kv._2, indentLevel + 1, formatting)
            }, ",\n")

            Iterator.single("{\n") ++
                fields ++
                Iterator.single("\n") ++
                indentation(indentLevel) ++ Iterator.single("}")
        } else {
            val fields = generateJoin(o.iterator, { kv : (String, JValue) =>
                Iterator(renderString(kv._1), ":") ++
                    render(kv._2, indentLevel + 1, formatting)
            }, ",")

            Iterator.single("{") ++
                fields ++
                Iterator.single("}")
        }
    }

    private def renderArray(a : JArray, indentLevel : Int, formatting : JsonFormatting.Value) : Iterator[String] = {
        if (formatting == JsonFormatting.Pretty) {
            val elements = generateJoin(a.iterator,
                { e : JValue => indentation(indentLevel + 1) ++ render(e, indentLevel + 1, formatting) },
                ",\n")
            Iterator.single("[\n") ++
                elements ++
                Iterator.single("\n") ++
                indentation(indentLevel) ++ Iterator.single("]")
        } else {
            val elements = generateJoin(a.iterator,
                { e : JValue => render(e, indentLevel + 1, formatting) },
                ",")
            Iterator.single("[") ++
                elements ++
                Iterator.single("]")
        }
    }

    /**
     * Render a JValue as a JSON document, as a series of strings that should be concatenated
     * or written out in the order provided. The iterator may be lazy, that's
     * why you get an iterator instead of a single string (in theory a large stream of JSON
     * need never be entirely in RAM). For most small cases the iterator is just wasteful
     * though. Anytime you end up just flattening into a single string, it'd be faster to
     * use a StringBuilder throughout instead of all these iterators. Might end up doing
     * that. This is a little odd.
     *
     * Caller must always put indentation in front of the first line; any extra
     * lines returned are indented according to indentLevel parameter (which should match
     * the level used for the first line).
     *
     * @param value the JSON value to render
     * @param indentLevel number of indentation steps (4 spaces each) for lines after first
     * @param formatting Pretty to use extra whitespace and newlines, Compact to be small
     * @return JSON document as a series of strings that should be concatenated
     */
    private[bson] def render(value : JValue, indentLevel : Int, formatting : JsonFormatting.Value) : Iterator[String] = {
        val rendered = value match {
            case o : JObject => renderObject(o, indentLevel, formatting)
            case a : JArray => renderArray(a, indentLevel, formatting)
            case BBoolean(b) => Iterator.single(if (b) "true" else "false")
            case BString(s) => Iterator.single(renderString(s))
            case BInt32(i) => Iterator.single(i.toString)
            case BInt64(i) => Iterator.single(i.toString)
            case BDouble(d) => Iterator.single(d.toString)
            case BNull => Iterator.single("null")
        }
        if (formatting == JsonFormatting.Pretty) {
            rendered ++ Iterator.single("\n")
        } else {
            rendered
        }
    }
}
