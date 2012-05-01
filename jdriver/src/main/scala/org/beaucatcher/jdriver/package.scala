package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import org.bson.BSONObject
import org.bson.BSONException
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, MongoException => JavaMongoException, _ }

package object jdriver {
    import JavaConversions._

    object Implicits {
        private[jdriver] class EnrichedContext(context : DriverContext) {
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

        private[jdriver] implicit def context2enriched(context : DriverContext) = new EnrichedContext(context)

        private[jdriver] implicit def asScalaBObject(bsonObj : BSONObject) = JavaConversions.asScalaBObject(bsonObj)

        private[jdriver] implicit def asScalaWriteResult(j : JavaWriteResult) : WriteResult = {
            WriteResult({ asScalaBObject(j.getLastError()) })
        }

        private[jdriver] implicit def asScalaCommandResult(j : JavaCommandResult) : CommandResult = {
            CommandResult({ asScalaBObject(j) })
        }
    }

    private[jdriver] def convertDocumentToJava[D](doc : D)(implicit encodeSupport : DocumentEncoder[D]) : DBObject = {
        encodeSupport match {
            case javaSupport : JavaDocumentEncoder[_] =>
                javaSupport.asInstanceOf[JavaDocumentEncoder[D]].toDBObject(doc)
            // TODO instead of this hack, put an EmptyDocument encoder in CollectionCodecSet
            case EmptyDocument.emptyDocumentQueryEncoder =>
                new BasicDBObject()
            case _ =>
                // we'll have to serialize from document then deserialize to Java,
                // which requires a non-Netty implementation of encode/decodebuffer
                throw new MongoException("Cannot use encoder with Java driver because it doesn't implement JavaDocumentEncoder: " + encodeSupport)
        }
    }

    private[jdriver] def convertQueryToJava[Q](query : Q)(implicit querySupport : QueryEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertModifierToJava[Q](query : Q)(implicit querySupport : ModifierEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertUpsertToJava[Q](query : Q)(implicit querySupport : UpsertEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertUpdateQueryToJava[Q](query : Q)(implicit querySupport : UpdateQueryEncoder[Q]) : DBObject = {
        convertDocumentToJava(query)
    }

    private[jdriver] def convertResultFromJava[E](obj : BSONObject)(implicit entitySupport : QueryResultDecoder[E]) : E = {
        entitySupport match {
            case javaSupport : JavaDocumentDecoder[_] =>
                javaSupport.asInstanceOf[JavaDocumentDecoder[E]].fromBsonObject(obj)
            case _ =>
                // we'll have to serialize from Java then deserialize to target, which requires
                // a non-Netty implementation of encode/decode buffer
                throw new MongoException("Cannot use decoder with Java driver because it doesn't implement JavaDocumentDecoder: " + entitySupport)
        }
    }

    private[jdriver] def convertValueFromJava[V](v : AnyRef)(implicit valueDecoder : ValueDecoder[V]) : V = {
        valueDecoder match {
            case javaDecoder : JavaValueDecoder[_] =>
                javaDecoder.asInstanceOf[JavaValueDecoder[V]].convert(v)
            case _ =>
                // we'll have to serialize from Java then deserialize to target, which requires
                // a non-Netty implementation of encode/decode buffer
                throw new MongoException("Cannot use value decoder with Java driver because it doesn't implement JavaValueDecoder: " + valueDecoder)
        }
    }

    private[jdriver] def putIdToJava[I](obj : BSONObject, name : String, id : I)(implicit idEncoder : IdEncoder[I]) : Unit = {
        idEncoder match {
            case javaEncoder : JavaIdEncoder[_] =>
                javaEncoder.asInstanceOf[JavaIdEncoder[I]].put(obj, name, id)
            case _ =>
                // we'll have to serialize from Java then deserialize to target, which requires
                // a non-Netty implementation of encode/decode buffer
                throw new MongoException("Cannot use ID encoder with Java driver because it doesn't implement JavaIdEncoder: " + idEncoder)
        }
    }
}
