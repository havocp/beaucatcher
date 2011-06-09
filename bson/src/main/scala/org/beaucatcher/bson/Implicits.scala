package org.beaucatcher.bson

import java.util.Date
import org.bson.types._
import org.bson.BSONObject
import org.joda.time._

/**
 * Object containing implicit conversions from primitive types into [[org.beaucatcher.bson.BValue]].
 * This saves a lot of explicit wrapping in [[org.beaucatcher.bson.BValue]], as in:
 * {{{
 *     import Implicits._
 *     BObject("name" -> "joe") == BObject("name" -> BString("joe"))
 * }}}
 */
object Implicits extends Implicits

/**
 * Trait containing implicit conversions from primitive types into [[org.beaucatcher.bson.BValue]].
 * You can derive from this trait or import `org.beaucatcher.bson.Implicits._`, whichever you prefer.
 */
trait Implicits {
    implicit def null2bvalue(x : Null) = BNull // this never works I guess because null is-a BValue anyway
    implicit def int2bvalue(x : Int) = BInt32(x)
    implicit def long2bvalue(x : Long) = BInt64(x)
    implicit def bigint2bvalue(x : BigInt) : BValue = if (x.isValidInt) BInt32(x.intValue) else BInt64(x.longValue)
    implicit def double2bvalue(x : Double) = BDouble(x)
    implicit def float2bvalue(x : Float) = BDouble(x)
    implicit def bigdecimal2bvalue(x : BigDecimal) = BDouble(x.doubleValue)
    implicit def boolean2bvalue(x : Boolean) = BBoolean(x)
    implicit def string2bvalue(x : String) = BString(x)
    implicit def date2bvalue(x : Date) = BISODate(new DateTime(x))
    implicit def datetime2bvalue(x : DateTime) = BISODate(x)
    implicit def timestamp2bvalue(x : BSONTimestamp) = BTimestamp(x)
    implicit def objectid2bvalue(x : ObjectId) = BObjectId(x)
    implicit def binary2bvalue(x : Binary) = BBinData(x)
    implicit def map2bvalue[K <: String, V <% BValue](x : Map[K, V]) = BObject(x)
    implicit def seq2bvalue[V <% BValue](x : Seq[V]) : BArray = BArray(x)
}
