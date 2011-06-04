package org.beaucatcher

import org.beaucatcher.bson._
import org.bson.BSONObject
import scalaj.collection.Implicits._

package object casbah {
    object Implicits {
        implicit def bsonObject2BObject(bsonObj : BSONObject) = {
            val keys = bsonObj.keySet().iterator().asScala
            val fields = for { key <- keys }
                yield (key, BValue.wrap(bsonObj.get(key)))
            BObject(fields.toList)
        }
    }
}
