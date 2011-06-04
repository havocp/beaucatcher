package com.ometer.bson

import BsonAST._
import com.ometer.ClassAnalysis
import org.apache.commons.codec.binary.Base64
import org.bson.types._
import org.joda.time.{ DateTime, DateTimeZone }
import scala.tools.scalap.scalax.rules.scalasig.{ Type, TypeRefType, Symbol }

private[bson] object BsonValidation {
    private class FieldValidationException(val fieldName : String, message : String)
        extends JsonValidationException(message)
    private class BadValueException(fieldName : String, val expectedType : String, val foundValue : JValue)
        extends FieldValidationException(fieldName, "Expecting %s:%s and found %s".format(fieldName, expectedType, foundValue))
    private class UnhandledTypeException(fieldName : String, val typeInfo : String)
        extends FieldValidationException(fieldName, "No support for type %s found for field %s".format(typeInfo, fieldName))
    private class MissingFieldException(missingField : String)
        extends FieldValidationException(missingField, "Field %s should be in the parsed object".format(missingField))

    private def bStringToBinary(fieldName : String, s : BString) : Binary = {
        try {
            val decoded = Base64.decodeBase64(s.value)
            new Binary(BsonSubtype.GENERAL.code, decoded)
        } catch {
            case e : Exception =>
                throw new BadValueException(fieldName, "Binary", s)
        }
    }

    private def bStringToObjectId(fieldName : String, s : BString) : ObjectId = {
        try {
            new ObjectId(s.value)
        } catch {
            case e : Exception =>
                throw new BadValueException(fieldName, "ObjectId", s)
        }
    }

    private def checkedSimpleValue(fieldName : String, symbol : Symbol, value : JValue) : BValue = {
        // value may be BNull but not null
        require(value != null)

        symbol.path match {

            /* First match all types that are the same in JValue and BValue */

            case "scala.Int" => value match {
                case n : BInt32 =>
                    n
                case n : BNumericValue[_] if n.isValidInt =>
                    BInt32(n.intValue)
                case _ =>
                    throw new BadValueException(fieldName, "Int", value)
            }
            case "scala.Long" => value match {
                case n : BInt64 =>
                    n
                case n : BNumericValue[_] =>
                    BInt64(n.longValue)
                case _ =>
                    throw new BadValueException(fieldName, "Long", value)
            }
            case "scala.Double" => value match {
                case n : BDouble =>
                    n
                case n : BNumericValue[_] =>
                    BDouble(n.doubleValue)
                case _ =>
                    throw new BadValueException(fieldName, "Double", value)
            }
            case "scala.Predef.String" => value match {
                case s : BString =>
                    s
                case _ =>
                    throw new BadValueException(fieldName, "String", value)
            }
            case "scala.Boolean" => value match {
                case b : BBoolean =>
                    b
                case _ =>
                    throw new BadValueException(fieldName, "Boolean", value)
            }

            /* Now match types that require conversion to BValue */

            case "org.bson.types.Binary" => value match {
                case s : BString =>
                    BBinData(bStringToBinary(fieldName, s))
                case _ =>
                    throw new BadValueException(fieldName, "Binary", value)
            }
            case "org.bson.types.ObjectId" => value match {
                case s : BString =>
                    BObjectId(bStringToObjectId(fieldName, s))
                case _ =>
                    throw new BadValueException(fieldName, "ObjectId", value)
            }
            case "org.bson.types.BSONTimestamp" => value match {
                case n : BNumericValue[_] =>
                    BTimestamp(new BSONTimestamp((n.longValue / 1000).intValue, (n.longValue % 1000).intValue))
                case _ =>
                    throw new UnhandledTypeException(fieldName, symbol.path)
            }
            case "org.joda.time.DateTime" => value match {
                case n : BNumericValue[_] =>
                    BISODate(new DateTime(n.longValue, DateTimeZone.UTC))
                case _ =>
                    throw new UnhandledTypeException(fieldName, symbol.path)
            }

            /* We don't know this type */

            case _ =>
                throw new UnhandledTypeException(fieldName, symbol.path)
        }
    }

    private def checkedMap(fieldName : String, valueType : Type, value : JObject) : BObject = {
        val b = BObject.newBuilder
        for ((k, v) <- value) {
            b += Pair(k, checkedValue(fieldName + "." + k, valueType, v))
        }
        b.result
    }

    private def checkedList(fieldName : String, elemType : Type, value : JArray) : BArray = {
        val b = BArray.newBuilder
        for ((elem, i) <- value zipWithIndex) {
            b += checkedValue(fieldName + "[" + i + "]", elemType, elem)
        }
        b.result
    }

    private def checkedValue(name : String, t : Type, value : JValue) : BValue = {
        (value, t) match {
            // Map
            case (v, TypeRefType(_, symbol, TypeRefType(_, kSymbol, _) :: vType :: Nil)) if symbol.path.endsWith(".Map") =>
                if (kSymbol.path != "scala.Predef.String") {
                    // non-string map key
                    throw new UnhandledTypeException(name, t.toString)
                }

                v match {
                    case j : JObject =>
                        checkedMap(name, vType, j)
                    case _ =>
                        throw new BadValueException(name, "Object", v)
                }

            // List
            case (v, TypeRefType(_, symbol, elemType :: Nil)) if symbol.path.endsWith(".List") =>
                v match {
                    case j : JArray =>
                        checkedList(name, elemType, j)
                    case _ =>
                        throw new BadValueException(name, "List", v)
                }

            // Non-collection types
            case (v, TypeRefType(_, symbol, _)) =>
                checkedSimpleValue(name, symbol, v)

            // Unhandled
            case _ =>
                throw new UnhandledTypeException(name, t.toString)
        }
    }

    private def validateObjectAgainstCaseClass[C <: Product](analysis : ClassAnalysis[C],
        rootObject : JObject,
        flavor : JsonFlavor) : BObject = {
        // this should be sure all fields in case class were found, and only
        // whitelist in those fields, so any "extra" fields just get dropped
        var validated = BObject.empty

        // we'll collect all per-field errors so people get them all in one go
        var missingFields = List[String]()
        var badValues = List[String]()
        var unhandledTypes = List[String]()
        var fieldExceptions = List[FieldValidationException]()

        for (((name, t), optional) <- analysis.fieldNamesIterator zip analysis.fieldTypesIterator zip analysis.fieldOptionalityIterator) {
            try {
                val vOption = rootObject.get(name)
                vOption match {
                    case Some(v) =>
                        val vType = if (optional) t.asInstanceOf[TypeRefType].typeArgs(0) else t
                        validated += (name, checkedValue(name, vType, v))
                    case None =>
                        if (!optional)
                            throw new MissingFieldException(name)
                }
            } catch {
                case e : BadValueException =>
                    badValues = "%s:%s=%s".format(name, e.expectedType, e.foundValue) :: badValues
                    fieldExceptions = e :: fieldExceptions
                case e : UnhandledTypeException =>
                    unhandledTypes = "%s:%s".format(name, e.typeInfo) :: unhandledTypes
                    fieldExceptions = e :: fieldExceptions
                case e : MissingFieldException =>
                    missingFields = name :: missingFields
                    fieldExceptions = e :: fieldExceptions
            }
        }

        if (!fieldExceptions.isEmpty) {
            if (fieldExceptions.length == 1) {
                // throw the original exception if there's just one
                throw fieldExceptions.head
            } else {
                // otherwise build a composite exception so
                // people know all problems at once
                val sb = new StringBuilder
                sb.append("Failed to validate: ")
                if (!missingFields.isEmpty) {
                    sb.append("Missing fields ")
                    missingFields.addString(sb, ", ")
                }
                if (!badValues.isEmpty) {
                    sb.append("Bad values ")
                    badValues.addString(sb, ", ")
                }
                if (!unhandledTypes.isEmpty) {
                    sb.append("Unhandled types ")
                    unhandledTypes.addString(sb, ", ")
                }
                throw new JsonValidationException(sb.toString)
            }
        }

        validated
    }

    def validateAgainstCaseClass[C <: Product](analysis : ClassAnalysis[C],
        value : JValue,
        flavor : JsonFlavor = JsonFlavor.CLEAN) : BValue = {
        require(flavor == JsonFlavor.CLEAN) // other flavors not supported for now
        value match {
            case jobject : JObject =>
                validateObjectAgainstCaseClass(analysis, jobject, flavor)
            case _ =>
                throw new Exception("JSON value is not an object, therefore does not validate for conversion to a case class")
        }
    }
}
