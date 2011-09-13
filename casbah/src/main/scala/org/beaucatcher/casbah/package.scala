package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import com.mongodb.{ WriteResult => JavaWriteResult }
import com.mongodb.{ CommandResult => JavaCommandResult }

package object casbah {
    import j.JavaConversions._

    object Implicits {
        private[casbah] implicit def asScalaBObject(bsonObj : BSONObject) = {
            import scala.collection.JavaConversions._

            val keys = bsonObj.keySet().iterator()
            val fields = for { key <- keys }
                yield (key, wrapJavaAsBValue(bsonObj.get(key)))
            BObject(fields.toList)
        }

        private[casbah] implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
            new WriteResult({ asScalaBObject(j.getLastError()) })
        }

        private[casbah] implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
            new CommandResult({ asScalaBObject(j) })
        }
    }
}
