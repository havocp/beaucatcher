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
package org.beaucatcher.bobject

import scala.io.Source

/** A JsonToken is one token, which can be returned by the JSON tokenizer. */
sealed abstract trait JsonToken

/**
 * Companion object for JsonToken, contains the specific token types
 * and the tokenize() method.
 */
object JsonToken {
    case object Start extends JsonToken
    case object End extends JsonToken
    case object PastEnd extends JsonToken

    case object Comma extends JsonToken
    case object Colon extends JsonToken

    case object OpenObject extends JsonToken
    case object CloseObject extends JsonToken
    case object OpenArray extends JsonToken
    case object CloseArray extends JsonToken

    abstract trait Value extends JsonToken
    case class StringValue(value: String) extends Value
    case class LongValue(value: Long) extends Value
    case class DoubleValue(value: Double) extends Value
    abstract class BooleanValue(value: Boolean) extends Value
    case object TrueValue extends BooleanValue(true)
    case object FalseValue extends BooleanValue(false)
    case object NullValue extends Value

    private def parseError(message: String, cause: Throwable = null) = {
        throw new JsonParseException(message, cause)
    }

    private def headOrNone(i: BufferedIterator[Char]): Option[Char] = if (i.hasNext) Some(i.head) else None
    private def nextOrElse(i: Iterator[Char], action: => Char): Char = if (i.hasNext) i.next else action
    private def nextIsOrThrow(input: Iterator[Char], expectedBefore: String, expectedNow: String): Unit = {
        for (c <- expectedNow) {
            val actual = nextOrElse(input, parseError("Expecting '%s%s' but input data ended".format(expectedBefore, expectedNow)))
            if (c != actual)
                parseError("Expecting '%s%s' but got char '%s' rather than '%s'".format(expectedBefore, expectedNow, actual, c))
        }
    }

    private def nextAfterWhitespace(i: Iterator[Char]): Option[Char] = {
        // this is clunky but not finding a nice pretty solution that seems efficient too
        if (i.hasNext) {
            var c = i.next
            // c == ' ' is supposed to be a cheesy optimization for common case
            while (c == ' ' || Character.isWhitespace(c)) {
                if (!i.hasNext)
                    return None // kind of gross return-from-loop, eh

                c = i.next
            }
            Some(c)
        } else {
            None
        }
    }

    private def pullString(input: Iterator[Char]): StringValue = {
        val sb = new java.lang.StringBuilder()
        var c = '\0' // value doesn't get used
        do {
            c = nextOrElse(input, parseError("End of input but string quote was still open"))
            c match {
                case '\\' =>
                    val escaped = nextOrElse(input, parseError("End of input but backslash in string had nothing after it"))
                    escaped match {
                        case '"' => sb.append('"')
                        case '\\' => sb.append('\\')
                        case '/' => sb.append('/')
                        case 'b' => sb.append('\b')
                        case 'f' => sb.append('\f')
                        case 'n' => sb.append('\n')
                        case 'r' => sb.append('\r')
                        case 't' => sb.append('\t')
                        case 'u' => {
                            def missingHexDigit = parseError("End of input but expecting 4 hex digits for \\uXXXX escape")

                            // kind of absurdly slow, but screw it for now
                            val a = new Array[Char](4)
                            for (i <- 0 to 3) {
                                a.update(i, nextOrElse(input, missingHexDigit))
                            }
                            val digits: String = new String(a)

                            try {
                                sb.appendCodePoint(Integer.parseInt(digits, 16))
                            } catch {
                                case e: NumberFormatException =>
                                    parseError("Malformed hex digits after \\u escape in string: '%s'".format(digits), e)
                            }
                        }
                        case whatever =>
                            parseError("backslash followed by '%s', this is not a valid escape sequence".format(whatever))
                    }
                case '"' => // we'll end the while loop. done!
                case _ => sb.append(c)
            }
        } while (c != '"')
        StringValue(sb.toString)
    }

    private def pullTrue(input: Iterator[Char]): JsonToken = {
        // "t" has been already seen
        nextIsOrThrow(input, "t", "rue")
        TrueValue
    }

    private def pullFalse(input: Iterator[Char]): JsonToken = {
        // "f" has already been seen
        nextIsOrThrow(input, "f", "alse")
        FalseValue
    }

    private def pullNull(input: Iterator[Char]): JsonToken = {
        // "n" has already been seen
        nextIsOrThrow(input, "n", "ull")
        NullValue
    }

    private def pullNumber(input: BufferedIterator[Char], firstC: Char): JsonToken = {
        val sb = new StringBuilder()
        sb.append(firstC)
        // we have to use head instead of next to peek, since we have no way to end
        // the token other than to see the first character that isn't the right kind
        var c = headOrNone(input).getOrElse('\0') // note, nul byte not digit 0
        var containedDecimalOrE = false
        while ("0123456789eE+-.".contains(c)) {
            if (c == '.' || c == 'e' || c == 'E')
                containedDecimalOrE = true
            sb.append(c)
            input.next // consume the iterator we just peeked
            c = headOrNone(input).getOrElse('\0')
        }
        val s = sb.toString
        try {
            if (containedDecimalOrE) {
                // force floating point representation
                DoubleValue(java.lang.Double.parseDouble(s))
            } else {
                // this should throw if the integer is too large for Long
                LongValue(java.lang.Long.parseLong(s))
            }
        } catch {
            case e: NumberFormatException =>
                parseError("Invalid number", e)
        }
    }

    private def pullNext(input: BufferedIterator[Char]): JsonToken = {
        val maybeC = nextAfterWhitespace(input)
        if (maybeC.isDefined) {
            maybeC.get match {
                case '"' => pullString(input)
                case ':' => Colon
                case ',' => Comma
                case '{' => OpenObject
                case '}' => CloseObject
                case '[' => OpenArray
                case ']' => CloseArray
                case 't' => pullTrue(input)
                case 'f' => pullFalse(input)
                case 'n' => pullNull(input)
                case c if "-0123456789".contains(c) => pullNumber(input, c)
                case whatever => parseError("Character '%s' is not the start of any valid JSON token".format(whatever))
            }
        } else {
            End
        }
    }

    /**
     * Tokenize a series of characters into JSON tokens. Performs no syntax or semantic checking
     * of relationship between tokens; only validates "within" tokens, such as checking that numbers
     * are valid numbers and strings are valid strings. But doesn't care whether numbers are next
     * to each other or not, it just gives you a raw jumble of tokens.
     * This is not necessarily useful for most purposes. Use the higher-level parsers.
     * @param input iterator over characters
     * @return iterator over JSON tokens
     */
    def tokenize(input: Iterator[Char]): Iterator[JsonToken] = {
        new Iterator[JsonToken]() {
            private val bufferedInput = input.buffered
            // the next token, we haven't returned it yet
            var token: JsonToken = Start
            override def next = {
                if (token == PastEnd)
                    throw new NoSuchElementException("No more JSON input data")
                val old = token
                if (old == End)
                    token = PastEnd
                else
                    token = pullNext(bufferedInput)
                old
            }
            override def hasNext = token != PastEnd
        }
    }
}
