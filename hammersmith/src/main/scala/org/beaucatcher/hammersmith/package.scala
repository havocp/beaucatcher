package org.beaucatcher

import org.bson.collection._
import akka.dispatch._
import java.util.concurrent.TimeUnit
import org.beaucatcher.bson._
import org.bson.SerializableBSONObject

package object hammersmith {
    private[hammersmith] def newPromise[T] = {
        // the default timeout appears to be zero, so we have to fix this
        // short is nicer for testing but long is probably better in real life
        new DefaultCompletableFuture[T](1, TimeUnit.SECONDS)
        // new DefaultCompletableFuture[T](15, TimeUnit.SECONDS)
    }

    private[hammersmith] class BObjectBSONDocument[ValueType <: BValue, Repr <: Map[String, ValueType]](obj : ObjectBase[ValueType, Repr]) extends BSONDocument {
        // we have to make a mutable map to make hammersmith happy
        // (but we're going to get rid of this whole class, so that will fix it)
        override val self = scala.collection.mutable.HashMap.empty ++ obj.unwrapped
        override def asMap = self
    }

    private[hammersmith] implicit object SerializableBSONDocument extends SerializableBSONDocumentLike[BSONDocument]

    private[hammersmith] implicit object SerializableBObject
        extends ObjectBaseSerializer[BValue, BObject, BObject] {
        override protected def construct(list : List[(String, BValue)]) : BObject = {
            BObject(list)
        }

        override protected def withNewField(doc : BObject, field : (String, BValue)) : BObject = {
            doc + field
        }

        override protected def wrapValue(x : Any) : BValue = {
            BValue.wrap(x)
        }
    }

    private[hammersmith] implicit object SerializableJObject
        extends ObjectBaseSerializer[JValue, JObject, JObject] {
        override protected def construct(list : List[(String, JValue)]) : JObject = {
            JObject(list)
        }

        override protected def withNewField(doc : JObject, field : (String, JValue)) : JObject = {
            doc + field
        }

        override protected def wrapValue(x : Any) : JValue = {
            BValue.wrap(x).toJValue()
        }
    }
}
