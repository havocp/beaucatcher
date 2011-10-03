package org.beaucatcher.bson

import org.bson.{ types => j }

object JavaConversions {
    implicit def asJavaObjectId(o : ObjectId) = new j.ObjectId(o.time, o.machine, o.inc)

    implicit def asScalaObjectId(o : j.ObjectId) = ObjectId(o.getTimeSecond(), o.getMachine(), o.getInc())

    implicit def asJavaTimestamp(t : Timestamp) = new j.BSONTimestamp(t.time, t.inc)

    implicit def asScalaTimestamp(t : j.BSONTimestamp) = Timestamp(t.getTime(), t.getInc())

    implicit def asJavaBinary(b : Binary) = new j.Binary(BsonSubtype.toByte(b.subtype), b.data)

    implicit def asScalaBinary(b : j.Binary) = Binary(b.getData(), BsonSubtype.fromByte(b.getType()).get)

    class JavaConvertibleBValue[SrcType <: BValue, JavaType <: AnyRef](src : SrcType) {
        import scala.collection.JavaConverters._

        private def unwrap(bvalue : BValue) : AnyRef = {
            bvalue match {
                case BBinary(b) =>
                    b : j.Binary
                case BTimestamp(t) =>
                    t : j.BSONTimestamp
                case BObjectId(oid) =>
                    oid : j.ObjectId
                case v : ArrayBase[_] =>
                    v.value.map({ e => unwrap(e) }).asJava
                case v : ObjectBase[_, _] =>
                    (Map() ++ v.value.map(field => (field._1, unwrap(field._2)))).asJava
                case v : BValue =>
                    // handles strings and ints and stuff
                    v.unwrapped.asInstanceOf[AnyRef]
            }
        }

        /**
         * The unwrapped version of the [[org.beaucatcher.bson.BValue]] as a plain Java type. For example,
         * while the `unwrapped` method on a `BObject` returns a Scala `Map`, the
         * `unwrappedAsJava` method returns a Java `Map`. This method also converts to
         * [[org.bson.types.ObjectId]] from [[org.beaucatcher.bson.ObjectId]] and similarly
         * for `Timestamp` and `Binary`. This is useful for interoperating with Java APIs.
         *
         * @return the unwrapped value as a Java type
         */
        def unwrappedAsJava : JavaType = {
            unwrap(src).asInstanceOf[JavaType]
        }
    }

    implicit def asJavaConvertibleBValue(bvalue : BValue) : JavaConvertibleBValue[BValue, AnyRef] =
        new JavaConvertibleBValue[BValue, AnyRef](bvalue)

    // These additional variants are so unwrappedAsJava returns the proper static type.
    // FIXME add variants for the rest of BValue subclasses (maybe one overload can do
    // all of BSingleValue though?)
    // each variant needs its own name because overloading confuses view bounds
    implicit def bobjectAsJavaConvertibleBValue(bobj : BObject) : JavaConvertibleBValue[BObject, java.util.Map[String, _]] =
        new JavaConvertibleBValue[BObject, java.util.Map[String, _]](bobj)
    implicit def barrayAsJavaConvertibleBValue(barray : BArray) : JavaConvertibleBValue[BArray, java.util.List[_]] =
        new JavaConvertibleBValue[BArray, java.util.List[_]](barray)

    /**
     * This is like `BValue.wrap()` but additionally can wrap [[org.bson.types]] types
     * and Java collections. Use `bvalue.unwrappedAsJava` to undo the wrap and get
     * a Java type.
     */
    def wrapJavaAsBValue(x : Any) : BValue = {
        import scala.collection.JavaConverters._

        x match {
            // null is in BValue.wrap too, but has to be here since
            // it matches all the cases below
            case null =>
                BNull
            case b : j.Binary =>
                BBinary(b)
            case t : j.BSONTimestamp =>
                BTimestamp(t)
            case oid : j.ObjectId =>
                BObjectId(oid)
            case m : java.util.Map[_, _] =>
                val builder = BObject.newBuilder
                for (k <- m.keySet.asScala) {
                    builder += (k.asInstanceOf[String] -> wrapJavaAsBValue(m.get(k)))
                }
                builder.result
            case l : java.util.List[_] =>
                val builder = BArray.newBuilder
                for (e <- l.asScala) {
                    builder += wrapJavaAsBValue(e)
                }
                builder.result
            case _ =>
                // Fall back to handle Scala types (including all the plain integers and so on)
                BValue.wrap(x)
        }
    }
}