package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import scalaj.collection.Implicits._
import com.mongodb.{ WriteResult => JavaWriteResult }
import com.mongodb.{ CommandResult => JavaCommandResult }

package object casbah {
    object Implicits {
        implicit def asScalaBObject(bsonObj : BSONObject) = {
            val keys = bsonObj.keySet().iterator().asScala
            val fields = for { key <- keys }
                yield (key, BValue.wrap(bsonObj.get(key)))
            BObject(fields.toList)
        }

        implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
            new WriteResult({ asScalaBObject(j.getLastError()) })
        }

        implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
            new CommandResult({ asScalaBObject(j) })
        }
    }
}
