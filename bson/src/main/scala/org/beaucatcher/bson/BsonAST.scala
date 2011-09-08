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

import scala.math.ScalaNumber
import scala.math.ScalaNumericConversions
import scala.collection.LinearSeqLike
import scala.collection.SeqLike
import scala.collection.mutable.Builder
import scala.collection.mutable.MapBuilder
import scala.collection.immutable.HashMap
import scala.collection.mutable.MapBuilder
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import java.util.Date
import org.apache.commons.codec.binary.Base64
import org.joda.time._
import scalaj.collection.Implicits._
import scala.io.Source

/**
 *  [[org.beaucatcher.bson.BValue]] is the base trait for all BSON value types. All [[org.beaucatcher.bson.BValue]]
 *  are immutable and the concrete subtypes of [[org.beaucatcher.bson.BValue]] are case classes.
 *
 *  See [[http://bsonspec.org/]] for more information about BSON.
 *
 *  The names of the subclasses implementing [[org.beaucatcher.bson.BValue]] mostly match the names in the MongoDB shell
 *  such as ObjectId, ISODate, except for numbers (because there are separate types for [[org.beaucatcher.bson.BInt32]],
 *  [[org.beaucatcher.bson.BInt64]], and [[org.beaucatcher.bson.BDouble]], while JavaScript has
 *  a single number type).
 *
 *  Most uses of [[org.beaucatcher.bson.BValue]] will start with the container [[org.beaucatcher.bson.BObject]], which implements the standard
 *  [[scala.collection.immutable.Map]] trait. There's also [[org.beaucatcher.bson.BArray]] which
 *  implements [[scala.collection.immutable.LinearSeq]].
 *
 *  A subset of [[org.beaucatcher.bson.BValue]] subtypes also implement the [[org.beaucatcher.bson.JValue]] trait, indicating that these types
 *  can appear in a JSON document, as well as in a BSON document.
 *
 *  [[org.beaucatcher.bson.JObject]] and [[org.beaucatcher.bson.JArray]] are separate types from [[org.beaucatcher.bson.BObject]]
 *  and [[org.beaucatcher.bson.BArray]] because they are guaranteed to contain only [[org.beaucatcher.bson.JValue]]
 *  elements, with no BSON-only values.
 *
 *  Import everything from [[org.beaucatcher.bson.Implicits]] if you want to auto-convert plain Scala types into
 *  BSON wrapper types. This lets you type nice BSON literals along these lines:
 *  {{{
 *    BObject("a" -> 42, "b" -> "hello world", "c" -> new ObjectId())
 *  }}}
 *
 *  Because the [[org.beaucatcher.bson.BValue]] subtypes are case classes, you can use pattern matching to unwrap them.
 */
sealed abstract trait BValue {
    /** The type you get if you unwrap the value */
    type WrappedType

    /**
     * The unwrapped version of the [[org.beaucatcher.bson.BValue]] (that is, a plain Scala type such as [[scala.Int]],
     * rather than a BSON type such as [[org.beaucatcher.bson.BInt32]]).
     * @return the unwrapped value
     */
    def unwrapped : WrappedType

    /**
     * The type "code" for this value's BSON type in the BSON/MongoDB wire protocol
     */
    val bsonType : BsonType.Value

    /**
     * The unwrapped version of the [[org.beaucatcher.bson.BValue]] as a plain Java type. For example,
     * while the `unwrapped` method on a `BObject` returns a Scala `Map`, the
     * `unwrappedAsJava` method returns a Java `Map`. This is useful for interoperating
     * with Java APIs.
     * @return the unwrapped value as a Java type
     */
    def unwrappedAsJava : AnyRef = {
        // this default implementation works for say String where Java and Scala are the same
        unwrapped.asInstanceOf[AnyRef]
    }

    /**
     * The value converted to a [[org.beaucatcher.bson.JValue]]. For many primitive types,
     * this method simply returns the same object. But types that exist in BSON but
     * not in JSON, such as [[org.beaucatcher.bson.BObjectId]], have to be mapped into
     * JSON's smaller set of types. The mapping is determined by the [[org.beaucatcher.bson.JsonFlavor]]
     * enumeration.
     *
     * @param flavor the style of mapping BSON to JSON see [[org.beaucatcher.bson.JsonFlavor]]
     * @return a JSON-only version of this value
     */
    def toJValue(flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : JValue

    /**
     * The value converted to a compact JSON string.
     *
     * @param flavor the style of mapping BSON to JSON see [[org.beaucatcher.bson.JsonFlavor]]
     * @return a JSON string representation of this value
     */
    def toJson(flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : String =
        BsonJson.toJson(this, flavor)

    /**
     * The value converted to a JSON string with nice formatting (i.e. with whitespace).
     *
     * @param flavor the style of mapping BSON to JSON see [[org.beaucatcher.bson.JsonFlavor]]
     * @return a nicely-formatted JSON string representation of this value
     */
    def toPrettyJson(flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : String =
        BsonJson.toPrettyJson(this, flavor)
}

/**
 *  This trait marks those [[org.beaucatcher.bson.BValue]] which are also valid in JSON.
 *  A subset of BSON values are also JSON values because
 *  they don't use extended BSON types.
 */
sealed abstract trait JValue extends BValue {

    // default implementation
    override def toJValue(flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : JValue = this
}

private[bson] sealed abstract class BSingleValue[T](override val bsonType : BsonType.Value, val value : T) extends BValue {
    type WrappedType = T
    override def unwrapped = value
}

/**
 * The value `null` in BSON and JSON.
 */
case object BNull extends JValue {
    type WrappedType = Null
    override val unwrapped = null
    override val bsonType = BsonType.NULL
}

/**
 * A BSON or JSON string value.
 */
case class BString(override val value : String) extends BSingleValue(BsonType.STRING, value) with JValue {
}

/**
 * Base class for numeric values. Implements [[scala.math.ScalaNumericConversions]] in order
 * to support conversion and comparison to numeric primitives.
 *
 *  @tparam T the underlying primitive type wrapped by this BSON node
 *  @param value the underlying primitive value
 */
sealed abstract class BNumericValue[T](override val bsonType : BsonType.Value, val value : T)
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

/**
 * A BSON or JSON floating-point value.
 */
case class BDouble(override val value : Double) extends BNumericValue(BsonType.NUMBER, value) {
    override def isWhole = (value % 1) == 0
    override def doubleValue = value

    override def hashCode() : Int = if (isWhole) unifiedPrimitiveHashcode else value.##
    override def equals(that : Any) : Boolean = unifiedBNumericEquals(that)
}

/**
 * A BSON or JSON 32-bit signed integer value.
 */
case class BInt32(override val value : Int) extends BNumericValue(BsonType.NUMBER_INT, value) {
    override def isWhole = true
    override def intValue = value

    override def hashCode() : Int = unifiedPrimitiveHashcode
    override def equals(that : Any) : Boolean = unifiedBNumericEquals(that)
}

/**
 * A BSON or JSON 64-bit signed integer value.
 */
case class BInt64(override val value : Long) extends BNumericValue(BsonType.NUMBER_LONG, value) {
    override def isWhole = true
    override def longValue = value

    override def hashCode() : Int = unifiedPrimitiveHashcode
    override def equals(that : Any) : Boolean = unifiedBNumericEquals(that)
}

/**
 * Common base class of [[org.beaucatcher.bson.BArray]] and [[org.beaucatcher.bson.JArray]], where
 * [[org.beaucatcher.bson.BArray]] holds any [[org.beaucatcher.bson.BValue]] and [[org.beaucatcher.bson.JArray]]
 * holds only [[org.beaucatcher.bson.JValue]].
 *
 * @tparam ElementType either [[org.beaucatcher.bson.BValue]] or [[org.beaucatcher.bson.JValue]]
 */
sealed abstract trait ArrayBase[+ElementType <: BValue] extends BValue
    with immutable.LinearSeq[ElementType] {
    override val bsonType = BsonType.ARRAY

    /**
     * The underlying list enclosed by this array. Note the subtle difference from
     * the `unwrapped` method: `unwrapped`
     * unwraps the elements recursively, while this is a list of wrapped values.
     */
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

/**
 * Trait implementing companion object functionality for [[org.beaucatcher.bson.ArrayBase]] subtypes.
 */
sealed trait ArrayBaseCompanion[ElementType <: BValue, Repr <: ArrayBase[ElementType]] {
    protected[bson] def construct(seq : Seq[ElementType]) : Repr
    protected[bson] def nullValue : ElementType

    /** An empty [[org.beaucatcher.bson.BArray]] or [[org.beaucatcher.bson.JArray]] */
    val empty : Repr = construct(List())

    /** Constructs an empty [[org.beaucatcher.bson.BArray]] or [[org.beaucatcher.bson.JArray]] */
    def apply() : Repr = {
        empty
    }

    /**
     * Constructs an array of length one containing the provided value.
     * @param v a value that can be wrapped in [[org.beaucatcher.bson.BValue]] for [[org.beaucatcher.bson.BArray]], [[org.beaucatcher.bson.JValue]] for [[org.beaucatcher.bson.JArray]]
     * @tparam V the type of the value
     */
    def apply[V <% ElementType](v : V) : Repr = {
        construct(List[ElementType](if (v == null) nullValue else v))
    }

    /**
     * Constructs a [[org.beaucatcher.bson.BArray]] or [[org.beaucatcher.bson.JArray]] containing the values in the sequence,
     * each wrapped in a [[org.beaucatcher.bson.BValue]] or [[org.beaucatcher.bson.JValue]].
     * @param seq a sequence of values convertible to [[org.beaucatcher.bson.BValue]] or [[org.beaucatcher.bson.JValue]]
     * @tparam V type of elements in the sequence
     */
    def apply[V <% ElementType](seq : Seq[V]) : Repr = {
        val bvalues = for { v <- seq }
            yield if (v == null) nullValue else v : ElementType
        construct(bvalues.toSeq)
    }

    /**
     * Constructs a [[org.beaucatcher.bson.BArray]] or [[org.beaucatcher.bson.JArray]] containing the listed
     * values as elements.
     *
     * @param v1 a first value
     * @param v2 a second value
     * @param vs optional additional value
     */
    def apply(v1 : ElementType, v2 : ElementType, vs : ElementType*) : Repr = {
        construct((if (v1 == null) nullValue else v1) ::
            (if (v2 == null) nullValue else v2) ::
            vs.map({ v => if (v == null) nullValue else v }).toList)
    }

    /**
     * Creates a builder object used to efficiently build a new [[org.beaucatcher.bson.Repr]]
     * @return a new builder for [[org.beaucatcher.bson.Repr]]
     */
    def newBuilder : Builder[ElementType, Repr]
}

/**
 * A BSON array of values. Implements [[scala.collection.LinearSeqLike]] so you can
 * use it like a regular Scala sequence of [[org.beaucatcher.bson.BValue]].
 */
case class BArray(override val value : List[BValue])
    extends ArrayBase[BValue]
    with LinearSeqLike[BValue, BArray] {

    require(value != null)

    override def toJValue(flavor : JsonFlavor.Value) : JValue = JArray(value.map(_.toJValue(flavor)))

    override def newBuilder = BArray.newBuilder
}

/**
 * Companion object for [[org.beaucatcher.bson.BArray]]
 */
object BArray extends ArrayBaseCompanion[BValue, BArray] {
    override def construct(elements : Seq[BValue]) : BArray = new BArray(elements.toList)
    override def nullValue : BValue = BNull
    override def newBuilder : Builder[BValue, BArray] = newArrayBuilder(list => BArray(list))

    implicit def canBuildFrom : CanBuildFrom[BArray, BValue, BArray] = {
        new CanBuildFrom[BArray, BValue, BArray] {
            def apply() : Builder[BValue, BArray] = newBuilder
            def apply(from : BArray) : Builder[BValue, BArray] = newBuilder
        }
    }
}

/**
 * Exactly like [[org.beaucatcher.bson.BArray]] but guaranteed to contain only
 * [[org.beaucatcher.bson.JValue]] (all JSON, no BSON types).
 */
case class JArray(override val value : List[JValue])
    extends ArrayBase[JValue]
    with LinearSeqLike[JValue, JArray]
    with JValue {

    require(value != null)

    // lift-json overrides equals() on JArray to ignore the array's order.
    // I don't understand that so am not copying it for now.

    override def newBuilder = JArray.newBuilder

    implicit def canBuildFrom : CanBuildFrom[JArray, JValue, JArray] = {
        new CanBuildFrom[JArray, JValue, JArray] {
            def apply() : Builder[JValue, JArray] = newBuilder
            def apply(from : JArray) : Builder[JValue, JArray] = newBuilder
        }
    }
}

/**
 * Companion object for [[org.beaucatcher.bson.JArray]]
 */
object JArray extends ArrayBaseCompanion[JValue, JArray] {
    override def construct(elements : Seq[JValue]) : JArray = new JArray(elements.toList)
    override def nullValue : JValue = BNull
    override def newBuilder : Builder[JValue, JArray] = newArrayBuilder(list => JArray(list))
}

/**
 * A BSON binary data value, wrapping [[org.beaucatcher.bson.Binary]].
 *
 * @param value the binary data
 */
case class BBinary(override val value : Binary) extends BSingleValue(BsonType.BINARY, value) {

    override def toJValue(flavor : JsonFlavor.Value) = {
        flavor match {
            case JsonFlavor.CLEAN =>
                BString(Base64.encodeBase64String(value.data))
            case JsonFlavor.STRICT =>
                JObject(List(("$binary", BString(Base64.encodeBase64String(value.data))),
                    ("$type", BString("%02x".format("%02x", (BsonSubtype.toByte(value.subtype) : Int) & 0xff)))))
            case _ =>
                throw new UnsupportedOperationException("Don't yet support JsonFlavor " + flavor)
        }
    }
}

/** Companion object for [[org.beaucatcher.bson.BBinary]]. */
object BBinary {
    def apply(data : Array[Byte], subtype : BsonSubtype.Value) : BBinary =
        BBinary(Binary(data, subtype))

    def apply(data : Array[Byte]) : BBinary = BBinary(Binary(data))
}

/** A BSON object ID value, wrapping [[org.beaucatcher.bson.ObjectId]]. */
case class BObjectId(override val value : ObjectId) extends BSingleValue(BsonType.OID, value) {
    override def toJValue(flavor : JsonFlavor.Value) = {
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

object BObjectId {
    def apply(string : String) : BObjectId =
        BObjectId(ObjectId(string))
}

/** A BSON or JSON boolean value. */
case class BBoolean(override val value : Boolean) extends BSingleValue(BsonType.BOOLEAN, value) with JValue {
}

/** A BSON date time value, wrapping [[org.joda.time.DateTime]]. */
case class BISODate(override val value : DateTime) extends BSingleValue(BsonType.DATE, value) {
    override def toJValue(flavor : JsonFlavor.Value) = {
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

/** A BSON timestamp value, wrapping [[org.beaucatcher.bson.Timestamp]]. */
case class BTimestamp(override val value : Timestamp) extends BSingleValue(BsonType.TIMESTAMP, value) {
    override def toJValue(flavor : JsonFlavor.Value) = {
        flavor match {
            case JsonFlavor.CLEAN =>
                BInt64(value.asLong)
            /* http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON is missing how to do timestamp for now */
            case _ =>
                throw new UnsupportedOperationException("Don't yet support JsonFlavor " + flavor)
        }
    }
}

object BTimestamp {
    def apply(time : Int, inc : Int) : BTimestamp = BTimestamp(Timestamp(time, inc))
}

/**
 * Common base class of [[org.beaucatcher.bson.BObject]] and [[org.beaucatcher.bson.JObject]], where
 * [[org.beaucatcher.bson.BObject]] holds any [[org.beaucatcher.bson.BValue]] and [[org.beaucatcher.bson.JObject]]
 * holds only [[org.beaucatcher.bson.JValue]].
 *
 * Implements [[scala.collection.immutable.Map]], where the keys are always strings.
 *
 * @tparam ValueType either [[org.beaucatcher.bson.BValue]] or [[org.beaucatcher.bson.JValue]]
 * @tparam Repr subtype's type, used to implement the [[scala.collection.generic.CanBuildFrom]] mechanism
 */
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

    private lazy val index : Option[Map[String, ValueType]] = {
        // for maps with more than 7 elements, we build an index.
        // otherwise we assume the cost of the index exceeds the
        // cost of O(n) searches.
        if (value.isDefinedAt(7))
            Some(immutable.HashMap[String, ValueType](value : _*))
        else
            None
    }

    // Map: Optionally returns the value associated with a key.
    override def get(key : String) : Option[ValueType] = {
        if (index.isDefined) {
            index.get.get(key)
        } else {
            value.find(_._1 == key) match {
                case Some(field) => Some(field._2)
                case None => None
            }
        }
    }

    // Map: Creates a new iterator over all key/value pairs of this map
    override def iterator : Iterator[(String, ValueType)] = {
        value.map(field => (field._1, field._2)).iterator
    }

    final private val unboxedClasses : Map[Class[_], Class[_]] = Map(
        classOf[java.lang.Integer] -> classOf[Int],
        classOf[java.lang.Double] -> classOf[Double],
        classOf[java.lang.Boolean] -> classOf[Boolean],
        classOf[java.lang.Long] -> classOf[Long],
        classOf[java.lang.Short] -> classOf[Short],
        classOf[java.lang.Character] -> classOf[Char],
        classOf[java.lang.Byte] -> classOf[Byte],
        classOf[java.lang.Float] -> classOf[Float],
        classOf[scala.runtime.BoxedUnit] -> classOf[Unit])

    // The trickiness here is that asInstanceOf[A] doesn't use the
    // manifest and thus doesn't do anything (no runtime type
    // check). So we have to do it by hand, special-casing
    // AnyVal primitives because Java Class.cast won't work
    // on them as desired.
    private def checkedCast[A <: Any : ClassManifest](value : Any) : A = {
        if (value == null) {
            if (classManifest[A] <:< classManifest[AnyVal]) {
                throw new ClassCastException("null can't be converted to AnyVal type " + classManifest[A])
            } else {
                null.asInstanceOf[A]
            }
        } else {
            val klass = value.asInstanceOf[AnyRef].getClass
            val unboxedClass = unboxedClasses.getOrElse(klass, klass)

            /* value and the return value are always boxed because that's how
             * an Any is passed around; type A we're casting to may be boxed or
             * unboxed. For example, value is always java.lang.Integer for
             * ints, but A could be java.lang.Integer OR scala Int. But
             * even if A is Int, the return value is really always a
             * java.lang.Integer, so we can leave the value boxed.
             */

            if (classManifest[A].erasure.isAssignableFrom(unboxedClass) ||
                classManifest[A].erasure.isAssignableFrom(klass)) {
                value.asInstanceOf[A]
            } else {
                throw new ClassCastException("Requested " + classManifest[A] + " but value is " + value + " with type " +
                    klass.getName)
            }
        }
    }

    /**
     * Gets an unwrapped value from the map, or throws [[java.util.NoSuchElementException]].
     * A more-convenient alternative to `obj.get(key).get.unwrapped.asInstanceOf[A]`.
     *
     * @tparam A type to cast to
     * @param key the key to get
     */
    def getUnwrappedAs[A : Manifest](key : String) : A = {
        get(key) match {
            case Some(bvalue) =>
                checkedCast[A](bvalue.unwrapped)
            case None =>
                throw new NoSuchElementException("Key not found in BSON object: " + key)
        }
    }
}

sealed trait ObjectBaseCompanion[ValueType <: BValue, Repr <: ObjectBase[ValueType, Repr] with immutable.MapLike[String, ValueType, Repr]] {
    protected[bson] def construct(list : List[Pair[String, ValueType]]) : Repr
    protected[bson] def nullValue : ValueType

    /** An empty object with size 0. */
    val empty = construct(List())

    /**
     * Construct a new *SON object from a [[scala.collection.Map]], where
     * the keys are strings and the values can be converted to [[org.beaucatcher.bson.BValue]]
     * or [[org.beaucatcher.bson.JValue]].
     */
    def apply[K <: String, V <% ValueType](m : Map[K, V]) : Repr = {
        val fields = for { (k, v) <- m }
            yield Pair[String, ValueType](k, if (v == null) nullValue else v)
        construct(fields.toList)
    }

    /**
     * Construct a new object from a list of
     * key-value pairs. Useful to support syntax such as:
     * {{{
     *    BObject("foo" -> 42, "bar" -> "hello world")
     * }}}
     *
     * This method has to require `BValue`, not `V<:BValue`.
     * Otherwise the type of `V` would be inferred to `Any` if mixing different
     * value types in a list of key-value pairs, and the method would not apply.
     */
    def apply[K <: String](pair1 : (K, ValueType), pair2 : (K, ValueType), pairs : (K, ValueType)*) : Repr = {
        val fields = for { (k, v) <- List(pair1, pair2) ++ pairs }
            yield (k, if (v == null) nullValue else v)
        construct(fields.toList)
    }

    /** Construct a new empty object */
    def apply() : Repr = {
        empty
    }

    /**
     * Construct a new object from a single key-value pair
     * where the value can be converted to [[org.beaucatcher.bson.BValue]]
     * or [[org.beaucatcher.bson.JValue]].
     *
     * This has to require V<%BValue, otherwise in
     * {{{
     *   BObject("foo" -> foo)
     * }}}
     * there's an ambiguous overload with the other
     * single-argument apply() flavors. Overload
     * selection happens before implicit conversion,
     * so we need an overload that matches unambiguously prior to
     * any implicits. That's what this overload provides.
     */
    def apply[K <: String, V <% ValueType](pair : (K, V)) : Repr = {
        val bvalue : ValueType = if (pair._2 == null) nullValue else pair._2
        construct(List((pair._1, bvalue)))
    }

    /**
     * Creates a new builder for efficiently generating a [[org.beaucatcher.bson.BObject]]
     * or [[org.beaucatcher.bson.JObject]]
     */
    def newBuilder : MapBuilder[String, ValueType, Repr] =
        new MapBuilder[String, ValueType, Repr](empty)
}

/**
 * A BSON object (document). [[org.beaucatcher.bson.BObject]] implements [[scala.collection.immutable.Map]]
 * with string keys and [[org.beaucatcher.bson.BValue]] values.
 *
 * [[org.beaucatcher.bson.BObject]] makes an additional guarantee beyond the usual [[scala.collection.immutable.Map]]
 * contract, which is that it's ordered. MongoDB relies on ordering in some cases.
 */
case class BObject(override val value : List[BField]) extends ObjectBase[BValue, BObject]
    with immutable.MapLike[String, BValue, BObject] {

    override def toJValue(flavor : JsonFlavor.Value) = JObject(value.map(field => (field._1, field._2.toJValue(flavor))))

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

    /**
     * Extra overload allows you to get back a [[org.beaucatcher.bson.BObject]] rather than a
     * generic [[scala.collection.immutable.Map]] when adding a key-value pair to the map.
     * @param kv the key-value pair to add
     * @return a new map with the new pair added (or replaced)
     */
    def +(kv : (String, BValue))(implicit bf : CanBuildFrom[BObject, (String, BValue), BObject]) : BObject = {
        val b = bf(empty)
        b ++= this
        b += kv
        b.result
    }
}

/** Companion object for [[org.beaucatcher.bson.BObject]]. */
object BObject extends ObjectBaseCompanion[BValue, BObject] {
    override def construct(list : List[Pair[String, BValue]]) : BObject = new BObject(list)
    override def nullValue : BValue = BNull

    implicit def canBuildFrom : CanBuildFrom[BObject, (String, BValue), BObject] = {
        new CanBuildFrom[BObject, (String, BValue), BObject] {
            def apply() : Builder[(String, BValue), BObject] = newBuilder
            def apply(from : BObject) : Builder[(String, BValue), BObject] = newBuilder
        }
    }
}

/**
 * Exactly like [[org.beaucatcher.bson.BObject]] but guaranteed to contain only
 * [[org.beaucatcher.bson.JValue]] (all JSON, no BSON types).
 */
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

/**
 * Companion object to [[org.beaucatcher.bson.JObject]]. See the documentation
 * for [[org.beaucatcher.bson.BObject]]'s companion object.
 */
object JObject extends ObjectBaseCompanion[JValue, JObject] {
    override def construct(list : List[Pair[String, JValue]]) : JObject = new JObject(list)
    override def nullValue : JValue = BNull

    implicit def canBuildFrom : CanBuildFrom[JObject, (String, JValue), JObject] = {
        new CanBuildFrom[JObject, (String, JValue), JObject] {
            def apply() : Builder[(String, JValue), JObject] = newBuilder
            def apply(from : JObject) : Builder[(String, JValue), JObject] = newBuilder
        }
    }
}

/** Companion object for [[org.beaucatcher.bson.BValue]]. */
object BValue {
    /**
     * Wraps an object in a [[org.beaucatcher.bson.BValue]]. Throws
     * [[java.lang.UnsupportedOperationException]] if wrapping the
     * object is not possible.
     *
     * Because this is a dynamic (untypesafe) wrap, it's always better to
     * use implicit conversions from [[org.beaucatcher.bson.Implicits]]
     * unless you have an Any with unknown type.
     * Typically this untypesafe method is needed for Java interoperability
     * because you can end up with a [[java.lang.Object]] that needs
     * wrapping, when dealing with say a Java collection.
     */
    def wrap(value : Any) : BValue = {
        // a maintenance headache because the big case statement
        // has to be kept in sync with all the implicits.
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
                BBinary(b)
            case t : Timestamp =>
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

    /**
     * Parse a JSON string, validating it against the given [[org.beaucatcher.bson.ClassAnalysis]].
     * The JSON string must contain all the fields found in the case class (unless the fields have
     * a [[scala.Option]] type). If the fields have a BSON type (such as [[org.beaucatcher.bson.ObjectId]])
     * then the returned [[org.beaucatcher.bson.BValue]] will contain BSON-only types such as
     * [[org.beaucatcher.bson.BObjectId]]. The types of the case class fields are used to decide how
     * to parse the JSON, for example if the `_id` field in the case class has type  [[org.beaucatcher.bson.ObjectId]],
     * then the parser knows to return a string in the JSON document as [[org.beaucatcher.bson.BObjectId]]
     * rather than [[org.beaucatcher.bson.BString]].
     *
     * @param json a JSON string
     * @param schema analysis of a case class
     * @param flavor expected [[org.beaucatcher.bson.JsonFlavor]] of the incoming JSON
     * @return parse tree of values
     */
    def parseJson(json : String, schema : ClassAnalysis[_ <: Product], flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : BValue =
        BsonValidation.validateAgainstCaseClass(schema, JValue.parseJson(json), flavor)

    /**
     * Parse JSON from a [[java.io.Reader]], validating it against the given [[org.beaucatcher.bson.ClassAnalysis]].
     * (See documentation on the other version of `parseJson()` that takes a string parameter.)
     */
    def parseJson(json : Source, schema : ClassAnalysis[_ <: Product]) : BValue =
        BsonValidation.validateAgainstCaseClass(schema, JValue.parseJson(json), JsonFlavor.CLEAN)

    /**
     * Parse JSON from a [[java.io.Reader]], validating it against the given [[org.beaucatcher.bson.ClassAnalysis]].
     * (See documentation on the other version of `parseJson()` that takes a string parameter.)
     */
    def parseJson(json : Source, schema : ClassAnalysis[_ <: Product], flavor : JsonFlavor.Value) : BValue =
        BsonValidation.validateAgainstCaseClass(schema, JValue.parseJson(json), flavor)

    /**
     * Converts a [[org.beaucatcher.bson.JValue]] to a [[org.beaucatcher.bson.BValue]] using
     * a case class as a schema to decide on destination types. For example, an object ID might
     * be a [[org.beaucatcher.bson.BString]] in the [[org.beaucatcher.bson.JValue]], and the
     * string would be parsed as an object ID and converted to [[org.beaucatcher.bson.BObjectId]].
     * The mapping from JSON to BSON varies with the [[org.beaucatcher.bson.JsonFlavor]].
     *
     * @param jvalue a JSON parse tree
     * @param schema analysis of a case class
     * @param flavor expected [[org.beaucatcher.bson.JsonFlavor]] of the incoming JSON
     * @return parse tree of values
     */
    def fromJValue(jvalue : JValue, schema : ClassAnalysis[_ <: Product], flavor : JsonFlavor.Value = JsonFlavor.CLEAN) : BValue =
        BsonValidation.validateAgainstCaseClass(schema, jvalue, flavor)
}

/**
 * Companion object for [[org.beaucatcher.bson.JValue]]
 */
object JValue {
    /**
     * Parses a JSON string into a parse tree.
     * @param json a JSON string
     * @return a parse tree
     */
    def parseJson(json : String) : JValue = BsonJson.fromJson(json)

    /**
     * Parses JSON from a [[scala.io.Source]] into a parse tree.
     * @param json a JSON string
     * @return a parse tree
     */
    def parseJson(json : Source) : JValue = BsonJson.fromJson(json)
}

