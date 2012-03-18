package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import org.bson.BSONException
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, MongoException => JavaMongoException, _ }

package object jdriver {
    import JavaConversions._

    object Implicits {
        private[jdriver] class EnrichedContext(context : Context) {
            def asJavaContext() = {
                context match {
                    case null =>
                        throw new BugInSomethingMongoException("null mongo.Context")
                    case j : JavaDriverContext =>
                        j
                    case wrong =>
                        throw new BugInSomethingMongoException("mongo.Context passed to jdriver is not from jdriver; context type is " + wrong.getClass.getSimpleName)
                }
            }
        }

        private[jdriver] implicit def context2enriched(context : Context) = new EnrichedContext(context)

        private[jdriver] implicit def asScalaBObject(bsonObj : BSONObject) = JavaConversions.asScalaBObject(bsonObj)

        private[jdriver] implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
            new WriteResult({ asScalaBObject(j.getLastError()) })
        }

        private[jdriver] implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
            new CommandResult({ asScalaBObject(j) })
        }
    }

    private[jdriver] trait BValueDBObject extends DBObject {
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
    private[jdriver] class BObjectDBObject(b : BObject = BObject.empty) extends BObjectBSONObject(b) with BValueDBObject {

    }

    private[jdriver] class BArrayDBObject(b : BArray = BArray.empty) extends BArrayBSONObject(b) with BValueDBObject {

    }

    private[jdriver] val jdriverExceptionMapper : PartialFunction[Throwable, MongoException] = {
        case ex : JavaMongoException.DuplicateKey => new DuplicateKeyMongoException(ex.getMessage, ex)
        case ex : JavaMongoException => new MongoException(ex.getMessage, ex)
        case ex : BSONException => new MongoException(ex.getMessage, ex)
    }
}
