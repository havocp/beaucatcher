package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, _ }

package object casbah {
    import j.JavaConversions._

    object Implicits {
        private[casbah] implicit def asScalaBObject(bsonObj : BSONObject) = j.JavaConversions.asScalaBObject(bsonObj)

        private[casbah] implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
            new WriteResult({ asScalaBObject(j.getLastError()) })
        }

        private[casbah] implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
            new CommandResult({ asScalaBObject(j) })
        }
    }

    private[casbah] trait BValueDBObject extends DBObject {
        private[this] var isPartial : Boolean = false

        override def isPartialObject() : Boolean = isPartial

        override def markAsPartialObject() : Unit = {
            isPartial = true
        }
    }

    /**
     * adds DBObject extensions to BSONObject.
     * This is an internal implementation class not exported by the library.
     */
    private[casbah] class BObjectDBObject(b : BObject = BObject.empty) extends j.BObjectBSONObject(b) with BValueDBObject {

    }

    private[casbah] class BArrayDBObject(b : BArray = BArray.empty) extends j.BArrayBSONObject(b) with BValueDBObject {

    }
}
