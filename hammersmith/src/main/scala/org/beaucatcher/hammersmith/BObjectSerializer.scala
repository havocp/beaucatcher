package org.beaucatcher.hammersmith

import java.io.InputStream
import org.bson.SerializableBSONObject
import org.bson.io.OutputBuffer
import org.beaucatcher.bson._
import org.bson.collection.SerializableBSONDocumentLike
import org.bson.collection.BSONDocument
import org.bson.types.ObjectId

private[hammersmith] trait ObjectBaseSerializer[ValueType <: BValue, Repr <: Map[String, ValueType], DocType <: ObjectBase[ValueType, Repr] with ValueType]
    extends SerializableBSONObject[DocType] {

    protected def construct(list : List[(String, ValueType)]) : DocType
    protected def withNewField(doc : DocType, field : (String, ValueType)) : DocType
    protected def wrapValue(x : Any) : ValueType

    // hack just to get this working, before we implement it for real.
    // this is not efficient.
    import org.beaucatcher.hammersmith.{ SerializableBSONDocument => delegate }

    final override def encode(doc : DocType, out : OutputBuffer) : Unit = {
        delegate.encode(new BObjectBSONDocument(doc), out)
    }

    final override def encode(doc : DocType) : Array[Byte] = {
        delegate.encode(new BObjectBSONDocument(doc))
    }

    private def convertFromBSONDocument(bsonDocument : BSONDocument) : DocType = {
        val fields = bsonDocument.toList map { kv =>
            val wrapped = kv._2 match {
                case subdoc : BSONDocument =>
                    convertFromBSONDocument(subdoc)
                case x =>
                    wrapValue(x)
            }
            (kv._1, wrapped)
        }
        construct(fields)
    }

    final override def decode(in : InputStream) : DocType = {
        convertFromBSONDocument(delegate.decode(in))
    }

    final override def checkObject(doc : DocType, isQuery : Boolean) : Unit = {
        delegate.checkObject(new BObjectBSONDocument(doc), isQuery)
    }

    final override def checkKeys(doc : DocType) : Unit = {
        delegate.checkKeys(new BObjectBSONDocument(doc))
    }

    final override def checkID(doc : DocType) : DocType = {
        if (doc.contains("_id"))
            doc
        else
            withNewField(doc, ("_id" -> wrapValue(new ObjectId())))
    }

    final override def _id(doc : DocType) : Option[AnyRef] = {
        doc.get("_id")
    }
}
