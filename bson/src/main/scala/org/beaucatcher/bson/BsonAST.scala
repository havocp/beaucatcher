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

package com.ometer.bson

import com.ometer.ClassAnalysis
import BsonEnums._
import java.io.Reader
import scala.math.ScalaNumber
import scala.math.ScalaNumericConversions
import scala.collection.LinearSeqLike
import scala.collection.mutable.ListBuffer
import scala.collection.SeqLike
import scala.collection.mutable.MapBuilder
import scala.collection.immutable.HashMap
import scala.collection.mutable.Builder
import scala.collection.mutable.MapBuilder
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import java.util.Date
import org.apache.commons.codec.binary.Base64
import org.bson.types._
import org.bson.BSONObject
import org.joda.time._
import scalaj.collection.Implicits._

/**
 *  The names of the case classes mostly match the names in the MongoDB shell
 *  such as ObjectId, ISODate, except for numbers which are done a little more
 *  clearly than JavaScript does it.
 */
object BsonAST {

    sealed abstract trait BValue {
        type WrappedType
        def unwrapped : WrappedType

        val bsonType : BsonType

        def unwrappedAsJava : AnyRef = {
            // this default implementation works for say String where Java and Scala are the same
            unwrapped.asInstanceOf[AnyRef]
        }

        def toJValue(flavor : JsonFlavor = JsonFlavor.CLEAN) : JValue

        def toJson(flavor : JsonFlavor = JsonFlavor.CLEAN) : String =
            BsonJson.toJson(this, flavor)

        def toPrettyJson(flavor : JsonFlavor = JsonFlavor.CLEAN) : String =
            BsonJson.toPrettyJson(this, flavor)
    }

    /**
     *  A subset of BSON values (BValue) are also JSON values (JValue) because
     *  they don't use extended BSON types.
     */
    sealed abstract trait JValue extends BValue {

        // default implementation
        override def toJValue(flavor : JsonFlavor = JsonFlavor.CLEAN) : JValue = this
    }

    private[bson] sealed abstract class BSingleValue[T](override val bsonType : BsonType, val value : T) extends BValue {
        type WrappedType = T
        override def unwrapped = value
    }

    case object BNull extends JValue {
        type WrappedType = Null
        override val unwrapped = null
        override val bsonType = BsonType.NULL
    }

    case class BString(override val value : String) extends BSingleValue(BsonType.STRING, value) with JValue {
    }

    private[bson] sealed abstract class BNumericValue[T](override val bsonType : BsonType, val value : T)
        extends ScalaNumber
        with ScalaNumericConversions with JValue {
        type WrappedType = T
        override def unwrapped = value

        override def underlying = value.asInstanceOf[Number]
        override def intValue = underlying.intValue
        override def longValue = underlying.longValue
        override def floatValue = underlying.floatValue
        override def doubleValue = underlying.doubleValue

        final protected def isPrimitiveNumber(x : Any) = {
            x match {
                case x : Char => true
                case x : Byte => true
                case x : Short => true
                case x : Int => true
                case x : Long => true
                case x : Float => true
                case x : Double => true
                case _ => false
            }
        }

        // we don't just override equals() here because
        // I'm worried the case class subclasses would
        // override it again.
        final protected def unifiedBNumericEquals(that : Any) = {
            if (isPrimitiveNumber(that)) {
                unifiedPrimitiveEquals(that)
            } else {
                that match {
                    case numeric : BNumericValue[_] =>
                        this.value == numeric.value
                    case _ =>
                        false
                }
            }
        }
    }

    case class BDouble(override val value : Double) extends BNumericValue(BsonType.NUMBER, value) {
        override def isWhole = (value % 1) == 0
        override def doubleValue = value

        override def hashCode() : Int = if (isWhole) unifiedPrimitiveHashcode else value.##
        override def equals(that : Any) : Boolean = unifiedBNumericEquals(that)
    }

    case class BInt32(override val value : Int) extends BNumericValue(BsonType.NUMBER_INT, value) {
        override def isWhole = true
        override def intValue = value

        override def hashCode() : Int = unifiedPrimitiveHashcode
        override def equals(that : Any) : Boolean = unifiedBNumericEquals(that)
    }

    case class BInt64(override val value : Long) extends BNumericValue(BsonType.NUMBER_LONG, value) {
        override def isWhole = true
        override def longValue = value

        override def hashCode() : Int = unifiedPrimitiveHashcode
        override def equals(that : Any) : Boolean = unifiedBNumericEquals(that)
    }

    sealed abstract trait ArrayBase[+ElementType <: BValue] extends BValue
        with immutable.LinearSeq[ElementType] {
        override val bsonType = BsonType.ARRAY
        val value : List[ElementType]

        type WrappedType = List[Any]
        override lazy val unwrapped = value.map(_.unwrapped)
        override def unwrappedAsJava = value.map(_.unwrappedAsJava).asJava

        // SeqLike: length
        override def length = value.length

        // SeqLike: apply
        override def apply(idx : Int) : ElementType = value.apply(idx)

        // IterableLike: iterator
        override def iterator : Iterator[ElementType] = value.iterator
    }

    private[bson] def newArrayBuilder[V <: BValue, A <: ArrayBase[V] : Manifest](fromList : List[V] => A) : Builder[V, A] = {
        new Builder[V, A] {
            val buffer = new ListBuffer[V]
            override def clear : Unit = buffer.clear
            override def result = fromList(buffer.result)
            override def +=(elem : V) = {
                buffer += elem
                this
            }
        }
    }

    case class BArray(override val value : List[BValue])
        extends ArrayBase[BValue]
        with LinearSeqLike[BValue, BArray] {

        override def toJValue(flavor : JsonFlavor) : JValue = JArray(value.map(_.toJValue(flavor)))

        override def newBuilder = BArray.newBuilder
    }

    object BArray {
        val empty : BArray = BArray(List())

        def apply() : BArray = {
            empty
        }

        def apply[V <% BValue](v : V) : BArray = {
            BArray(List[BValue](if (v == null) BNull else v))
        }

        def apply[V <% BValue](seq : Seq[V]) : BArray = {
            val bvalues = for { v <- seq }
                yield if (v == null) BNull else v : BValue
            BArray(bvalues.toList)
        }

        def apply(v1 : BValue, v2 : BValue, vs : BValue*) : BArray = {
            BArray((if (v1 == null) BNull else v1) ::
                (if (v2 == null) BNull else v2) ::
                vs.map({ v => if (v == null) BNull else v }).toList)
        }

        def newBuilder : Builder[BValue, BArray] = newArrayBuilder(list => BArray(list))

        implicit def canBuildFrom : CanBuildFrom[BArray, BValue, BArray] = {
            new CanBuildFrom[BArray, BValue, BArray] {
                def apply() : Builder[BValue, BArray] = newBuilder
                def apply(from : BArray) : Builder[BValue, BArray] = newBuilder
            }
        }
    }

    case class JArray(override val value : List[JValue])
        extends ArrayBase[JValue]
        with LinearSeqLike[JValue, JArray]
        with JValue {

        // lift-json overrides equals() on JArray to ignore the array's order.
        // I don't understand that so am not copying it for now.

        override def newBuilder = JArray.newBuilder
    }

    object JArray {
        val empty : JArray = JArray(List())

        def apply() : JArray = {
            empty
        }

        def apply[V <% JValue](v : V) : JArray = {
            JArray(List[JValue](if (v == null) BNull else v))
        }

        def apply[V <% JValue](seq : Seq[V]) : JArray = {
            val jvalues = for { v <- seq }
                yield if (v == null) BNull else v : JValue
            JArray(jvalues.toList)
        }

        def apply(v1 : JValue, v2 : JValue, vs : JValue*) : JArray = {
            JArray((if (v1 == null) BNull else v1) ::
                (if (v2 == null) BNull else v2) ::
                vs.map({ v => if (v == null) BNull else v }).toList)
        }

        def newBuilder : Builder[JValue, JArray] = newArrayBuilder(list => JArray(list))

        implicit def canBuildFrom : CanBuildFrom[JArray, JValue, JArray] = {
            new CanBuildFrom[JArray, JValue, JArray] {
                def apply() : Builder[JValue, JArray] = newBuilder
                def apply(from : JArray) : Builder[JValue, JArray] = newBuilder
            }
        }
    }

    case class BBinData(val value : Array[Byte], val subtype : BsonSubtype) extends BValue {
        type WrappedType = Binary
        override lazy val unwrapped = new Binary(subtype.code, value)
        override val bsonType = BsonType.BINARY

        override def toJValue(flavor : JsonFlavor) = {
            flavor match {
                case JsonFlavor.CLEAN =>
                    BString(Base64.encodeBase64String(value))
                case JsonFlavor.STRICT =>
                    JObject(List(("$binary", BString(Base64.encodeBase64String(value))),
                        ("$type", BString("%02x".format("%02x", (subtype.code : Int) & 0xff)))))
                case _ =>
                    throw new UnsupportedOperationException("Don't yet support JsonFlavor " + flavor)
            }
        }

        // We have to fix equals() because default doesn't implement it
        // correctly (does not consider the contents of the byte[])
        override def equals(other : Any) : Boolean = {
            other match {
                case that : BBinData =>
                    (that canEqual this) &&
                        (subtype == that.subtype) &&
                        (value.length == that.value.length) &&
                        (value sameElements that.value)
                case _ => false
            }
        }

        // have to make hashCode match equals (array hashCode doesn't
        // look at elements, Seq hashCode does
        override def hashCode() : Int = {
            41 * (41 + subtype.hashCode) + (value : Seq[Byte]).hashCode
        }

        private def bytesAsString(sb : StringBuilder, i : Traversable[Byte]) = {
            for (b <- i) {
                sb.append("%02x".format((b : Int) & 0xff))
            }
        }

        // default toString just shows byte[] object id
        override def toString() : String = {
            val sb = new StringBuilder
            sb.append("BBinData(")
            val bytes = value.take(10)
            bytesAsString(sb, bytes)
            if (value.length > 10)
                sb.append("...")
            sb.append("@")
            sb.append(value.length.toString)
            sb.append(",")
            sb.append(subtype.toString)
            sb.append(")")
            sb.toString
        }
    }

    object BBinData {
        def apply(b : Binary) : BBinData = {
            BBinData(b.getData(), BsonSubtype.fromByte(b.getType()).get)
        }
    }

    case class BObjectId(override val value : ObjectId) extends BSingleValue(BsonType.OID, value) {
        override def toJValue(flavor : JsonFlavor) = {
            flavor match {
                case JsonFlavor.CLEAN =>
                    BString(value.toString())
                case JsonFlavor.STRICT =>
                    JObject(List(("$oid", BString(value.toString()))))
                case _ =>
                    throw new UnsupportedOperationException("Don't yet support JsonFlavor " + flavor)
            }
        }
    }

    case class BBoolean(override val value : Boolean) extends BSingleValue(BsonType.BOOLEAN, value) with JValue {
    }

    case class BISODate(override val value : DateTime) extends BSingleValue(BsonType.DATE, value) {
        override def toJValue(flavor : JsonFlavor) = {
            flavor match {
                case JsonFlavor.CLEAN =>
                    BInt64(value.getMillis())
                case JsonFlavor.STRICT =>
                    JObject(("$date", BInt64(value.getMillis())))
                case _ =>
                    throw new UnsupportedOperationException("Don't yet support JsonFlavor " + flavor)
            }
        }
    }

    case class BTimestamp(override val value : BSONTimestamp) extends BSingleValue(BsonType.TIMESTAMP, value) {
        override def toJValue(flavor : JsonFlavor) = {
            flavor match {
                case JsonFlavor.CLEAN =>
                    // convert to milliseconds and treat the "increment" as milliseconds after
                    // the round number of seconds.
                    val asInteger = (value.getTime * 1000L) | value.getInc
                    BInt64(asInteger)
                /* http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON is missing how to do timestamp for now */
                case _ =>
                    throw new UnsupportedOperationException("Don't yet support JsonFlavor " + flavor)
            }
        }
    }

    /**
     * lift-json seems wrong to make JField a JValue, because in JSON
     * (or BSON) a field is not a value. For example you can't
     * have an array of fields. So we don't derive BField from BValue.
     */
    private type Field[ValueType <: BValue] = Pair[String, ValueType]
    private type BField = Field[BValue]
    private type JField = Field[JValue]

    abstract trait ObjectBase[ValueType <: BValue, Repr <: Map[String, ValueType]]
        extends BValue
        with immutable.Map[String, ValueType] {
        type WrappedType = Map[String, Any]

        val value : List[Field[ValueType]]

        // it would be nice to use an ordered map like LinkedHashMap here,
        // but the only ordered map in standard collections is mutable, so
        // it isn't 100% no-brainer to use that.
        override lazy val unwrapped = Map() ++ value.map(field => (field._1, field._2.unwrapped : Any))
        override def unwrappedAsJava = (Map() ++ value.map(field => (field._1, field._2.unwrappedAsJava))).asJava
        override val bsonType = BsonType.OBJECT

        protected[this] def construct(list : List[Field[ValueType]]) : Repr

        protected[this] def withAddedField(field : Field[ValueType]) : Repr = {
            // the basic assumption here is that the list will be short
            // so the O(n) is acceptable in exchange for a small simple
            // data structure. we'll see I guess.
            val withoutOld = value.filter(_._1 != field._1)
            // we accept O(n) again to keep the fields in order
            construct(withoutOld :+ field)
        }

        // Map: builder
        // I can't come up with a unit test that fails without this, so commented out
        //override def newBuilder : MapBuilder[String, ValueType, Repr]

        // Map: Removes a key from this map, returning a new map.
        override def -(key : String) : Repr = {
            construct(value.filter(_._1 != key))
        }

        // Map: Optionally returns the value associated with a key.
        override def get(key : String) : Option[ValueType] = {
            value.find(_._1 == key) match {
                case Some(field) => Some(field._2)
                case None => None
            }
        }

        // Map: Creates a new iterator over all key/value pairs of this map
        override def iterator : Iterator[(String, ValueType)] = {
            value.map(field => (field._1, field._2)).iterator
        }

        def getUnwrappedAs[A : Manifest](key : String) : A = {
            get(key) match {
                case Some(bvalue) =>
                    // FIXME I don't know if the asInstanceOf will really do anything
                    // or if it just gets erased
                    bvalue.unwrapped.asInstanceOf[A]
                case None =>
                    throw new NoSuchElementException("Key not found in BSON object: " + key)
            }
        }
    }

    /**
     * BObject implements Map so we get all those convenient APIs.
     */
    case class BObject(override val value : List[BField]) extends ObjectBase[BValue, BObject]
        with immutable.MapLike[String, BValue, BObject] {

        override def toJValue(flavor : JsonFlavor) = JObject(value.map(field => (field._1, field._2.toJValue(flavor))))

        // lift-json fixes JObject equals() to ignore ordering; which makes
        // sense for JSON, but in BSON sometimes order matters.

        override def construct(list : List[Field[BValue]]) : BObject = BObject(list)

        // Map: empty map
        override lazy val empty : BObject = BObject.empty

        // Map: Adds a key/value pair to this map, returning a new map.
        override def +[B1 >: BValue](kv : (String, B1)) : Map[String, B1] = {
            kv match {
                case (key, bvalue : BValue) =>
                    withAddedField(Pair(key, bvalue))
                case _ =>
                    (Map() ++ this) + kv
            }
        }

        def +(kv : (String, BValue))(implicit bf : CanBuildFrom[BObject, (String, BValue), BObject]) : BObject = {
            val b = bf(empty)
            b ++= this
            b += kv
            b.result
        }
    }

    object BObject {
        val empty = new BObject(List())

        def apply(bsonObj : BSONObject) : BObject = {
            val keys = bsonObj.keySet().iterator().asScala
            val fields = for { key <- keys }
                yield (key, BValue.wrap(bsonObj.get(key)))
            BObject(fields.toList)
        }

        def apply[K <: String, V <% BValue](m : Map[K, V]) : BObject = {
            val fields = for { (k, v) <- m }
                yield Pair[String, BValue](k, if (v == null) BNull else v)
            BObject(fields.toList)
        }

        /* This has to require BValue, not V<:BValue, to support:
         *  BObject("foo" -> foo, "bar" -> bar)
         * Otherwise the type of V would be inferred to Any and
         * then it would not be a subclass of BValue.
         */
        def apply[K <: String](pair1 : (K, BValue), pair2 : (K, BValue), pairs : (K, BValue)*) : BObject = {
            val fields = for { (k, v) <- List(pair1, pair2) ++ pairs }
                yield (k, if (v == null) BNull else v)
            new BObject(fields.toList)
        }

        def apply() : BObject = {
            empty
        }

        /* This has to require V<%BValue, otherwise in
         *  BObject("foo" -> foo)
         * there's an ambiguous overload with the other 
         * single-argument apply() flavors. Overload
         * selection happens before implicit conversion,
         * so we need an overload that matches unambiguously prior to 
         * any implicits. That's what this overload provides.
         */
        def apply[K <: String, V <% BValue](pair : (K, V)) : BObject = {
            val bvalue : BValue = if (pair._2 == null) BNull else pair._2
            BObject(List((pair._1, bvalue)))
        }

        def newBuilder : MapBuilder[String, BValue, BObject] =
            new MapBuilder[String, BValue, BObject](empty)

        implicit def canBuildFrom : CanBuildFrom[BObject, (String, BValue), BObject] = {
            new CanBuildFrom[BObject, (String, BValue), BObject] {
                def apply() : Builder[(String, BValue), BObject] = newBuilder
                def apply(from : BObject) : Builder[(String, BValue), BObject] = newBuilder
            }
        }
    }

    case class JObject(override val value : List[JField]) extends ObjectBase[JValue, JObject]
        with immutable.MapLike[String, JValue, JObject]
        with JValue {

        // Maybe equals() on JObject should ignore order, while on BObject
        // it should consider order? probably not a good idea.
        // Order at least matters for JObject when converting to BObject...

        override def construct(list : List[Field[JValue]]) : JObject = JObject(list)

        override lazy val empty : JObject = JObject.empty

        override def +[B1 >: JValue](kv : (String, B1)) : Map[String, B1] = {
            kv match {
                case (key, jvalue : JValue) =>
                    withAddedField(Pair(key, jvalue))
                case _ =>
                    (Map() ++ this) + kv
            }
        }

        def +(kv : (String, JValue))(implicit bf : CanBuildFrom[JObject, (String, JValue), JObject]) : JObject = {
            val b = bf(empty)
            b ++= this
            b += kv
            b.result
        }
    }

    object JObject {
        val empty = new JObject(List())

        def apply[K <: String, V <% JValue](m : Map[K, V]) : JObject = {
            val fields = for { (k, v) <- m }
                yield Pair[String, JValue](k, if (v == null) BNull else v)
            JObject(fields.toList)
        }

        def apply[K <: String](pair1 : (K, JValue), pair2 : (K, JValue), pairs : (K, JValue)*) : JObject = {
            val fields = for { (k, v) <- List(pair1, pair2) ++ pairs }
                yield (k, if (v == null) BNull else v)
            new JObject(fields.toList)
        }

        def apply() : JObject = {
            empty
        }

        def apply[K <: String, V <% JValue](pair : (K, V)) : JObject = {
            val bvalue : JValue = if (pair._2 == null) BNull else pair._2
            JObject(List((pair._1, bvalue)))
        }

        def newBuilder : MapBuilder[String, JValue, JObject] =
            new MapBuilder[String, JValue, JObject](empty)

        implicit def canBuildFrom : CanBuildFrom[JObject, (String, JValue), JObject] = {
            new CanBuildFrom[JObject, (String, JValue), JObject] {
                def apply() : Builder[(String, JValue), JObject] = newBuilder
                def apply(from : JObject) : Builder[(String, JValue), JObject] = newBuilder
            }
        }
    }

    object BValue {
        // this is a dynamic (untypesafe) wrap, it's always better to
        // use implicit conversions unless you have an Any with unknown
        // type. basically this is needed for Java interoperability.
        // it's a maintenance headache because the big case statement
        // has to be kept in sync with all the implicits.
        def wrap(value : Any) : BValue = {
            value match {
                case null =>
                    BNull
                // allowing BValue to be mixed in to a map or list of tuples
                // means you can force-specify rather than relying on implicit
                // conversion if you need to. For example ("foo", 10) or ("foo", BDouble(10))
                case bvalue : BValue =>
                    bvalue
                case d : DateTime =>
                    BISODate(d)
                case d : Date =>
                    BISODate(new DateTime(d))
                case oid : ObjectId =>
                    BObjectId(oid)
                case b : Binary =>
                    BBinData(b)
                case t : BSONTimestamp =>
                    BTimestamp(t)
                case b : Boolean =>
                    BBoolean(b)
                case s : String =>
                    BString(s)
                case d : Double =>
                    BDouble(d)
                case i : Long =>
                    BInt64(i)
                case i : Int =>
                    BInt32(i)
                case i : BigInt =>
                    if (i.isValidInt) BInt32(i.intValue) else BInt64(i.longValue)
                case bsonObj : BSONObject =>
                    BObject(bsonObj)
                case m : Map[_, _] =>
                    BObject(m.iterator.map(kv => (kv._1.asInstanceOf[String], BValue.wrap(kv._2))).toList)
                case seq : Seq[_] =>
                    BArray(seq.map(BValue.wrap(_)).toList)
                // we can wrap Java types too, so unwrappedAsJava can be undone
                case m : java.util.Map[_, _] =>
                    wrap(m.asScala)
                case l : java.util.List[_] =>
                    wrap(l.asScala)
                case _ =>
                    throw new UnsupportedOperationException("Cannot convert to BValue: " + value)
            }
        }

        def parseJson(json : String, schema : ClassAnalysis[_ <: Product], flavor : JsonFlavor = JsonFlavor.CLEAN) : BValue =
            BsonValidation.validateAgainstCaseClass(schema, JValue.parseJson(json), flavor)

        def parseJson(json : Reader, schema : ClassAnalysis[_ <: Product]) : BValue =
            BsonValidation.validateAgainstCaseClass(schema, JValue.parseJson(json), JsonFlavor.CLEAN)

        def parseJson(json : Reader, schema : ClassAnalysis[_ <: Product], flavor : JsonFlavor) : BValue =
            BsonValidation.validateAgainstCaseClass(schema, JValue.parseJson(json), flavor)

        def fromJValue(jvalue : JValue, schema : ClassAnalysis[_ <: Product], flavor : JsonFlavor = JsonFlavor.CLEAN) : BValue =
            BsonValidation.validateAgainstCaseClass(schema, jvalue, flavor)
    }

    /* Mutable BSONObject/DBObject implementation used to save to MongoDB API */
    class BObjectBSONObject extends BSONObject {
        private[this] var bvalue : BObject = BObject.empty

        def this(b : BObject) = {
            this()
            bvalue = b
        }

        /* BSONObject interface */
        override def containsField(s : String) : Boolean = {
            bvalue.contains(s)
        }
        override def containsKey(s : String) : Boolean = containsField(s)

        override def get(key : String) : AnyRef = {
            bvalue.get(key) match {
                case Some(bvalue) =>
                    bvalue.unwrappedAsJava
                case None =>
                    null
            }
        }

        override def keySet() : java.util.Set[String] = {
            bvalue.keySet.asJava
        }

        // returns previous value
        override def put(key : String, v : AnyRef) : AnyRef = {
            val previous = get(key)
            bvalue = bvalue + (key, BValue.wrap(v))
            previous
        }

        override def putAll(bsonObj : BSONObject) : Unit = {
            for { key <- bsonObj.keySet() }
                put(key, BValue.wrap(bsonObj.get(key)))
        }

        override def putAll(m : java.util.Map[_, _]) : Unit = {
            for { key <- m.keySet() }
                put(key.asInstanceOf[String], BValue.wrap(m.get(key)))
        }

        override def removeField(key : String) : AnyRef = {
            val previous = get(key)
            bvalue = bvalue - key
            previous
        }

        override def toMap() : java.util.Map[_, _] = {
            bvalue.unwrappedAsJava
        }
    }

    object JValue {
        def parseJson(json : String) : JValue = BsonJson.fromJson(json)

        def parseJson(json : Reader) : JValue = BsonJson.fromJson(json)
    }
}
