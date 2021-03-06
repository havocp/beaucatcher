package org.beaucatcher

import scala.collection.mutable.Builder
import scala.collection.mutable.ListBuffer

package object bobject {
    private[bobject] def newArrayBuilder[V <: BValue, A <: ArrayBase[V]: Manifest](fromList: List[V] => A): Builder[V, A] = {
        new Builder[V, A] {
            val buffer = new ListBuffer[V]
            override def clear: Unit = buffer.clear
            override def result = fromList(buffer.result)
            override def +=(elem: V) = {
                buffer += elem
                this
            }
        }
    }

    /**
     * lift-json seems wrong to make JField a JValue, because in JSON
     * (or BSON) a field is not a value. For example you can't
     * have an array of fields. So we don't derive BField from BValue.
     */
    private[bobject]type Field[ValueType <: BValue] = Pair[String, ValueType]
    private[bobject]type BField = Field[BValue]
    private[bobject]type JField = Field[JValue]
}
