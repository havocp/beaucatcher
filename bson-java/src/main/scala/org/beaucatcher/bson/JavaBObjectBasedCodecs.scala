package org.beaucatcher.bson

import org.beaucatcher.mongo._
import com.mongodb.DBObject
import org.bson.BSONObject

trait JavaBObjectBasedCodecs[E] extends BObjectBasedCodecs[E] {

    override implicit def queryEncoder : QueryEncoder[E] with JavaDocumentEncoder[E]

    override implicit def queryResultDecoder : QueryResultDecoder[E] with JavaDocumentDecoder[E]

    override implicit def updateQueryEncoder : UpdateQueryEncoder[E] with JavaDocumentEncoder[E]

    override implicit def upsertEncoder : UpsertEncoder[E] with JavaDocumentEncoder[E]

    override implicit def modifierEncoder : ModifierEncoder[E] with JavaDocumentEncoder[E]
}

private final class JavaBObjectBasedCodecsImpl[E](toBObject : (E) => BObject,
    fromBObject : (BObject) => E)(implicit bobjectQueryEncoder : QueryEncoder[BObject] with JavaDocumentEncoder[BObject],
        bobjectQueryResultDecoder : QueryResultDecoder[BObject] with JavaDocumentDecoder[BObject],
        bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject] with JavaDocumentEncoder[BObject],
        bobjectUpsertEncoder : UpsertEncoder[BObject] with JavaDocumentEncoder[BObject],
        bobjectModifierEncoder : ModifierEncoder[BObject] with JavaDocumentEncoder[BObject])
    extends JavaBObjectBasedCodecs[E] {

    override lazy val queryEncoder = new QueryEncoder[E]() with JavaDocumentEncoder[E] {
        override def toDBObject(t : E) : DBObject = {
            val bobj = toBObject(t)
            bobjectQueryEncoder.toDBObject(bobj)
        }
    }

    override lazy val queryResultDecoder = new QueryResultDecoder[E]() with JavaDocumentDecoder[E] {
        override def fromBsonObject(obj : BSONObject) : E = {
            val bobj = bobjectQueryResultDecoder.fromBsonObject(obj)
            fromBObject(bobj)
        }
    }

    override lazy val updateQueryEncoder = new UpdateQueryEncoder[E]() with JavaDocumentEncoder[E] {
        override def toDBObject(t : E) : DBObject = {
            val bobj = toBObject(t)
            bobjectUpdateQueryEncoder.toDBObject(bobj)
        }
    }

    override lazy val upsertEncoder = new UpsertEncoder[E]() with JavaDocumentEncoder[E] {
        override def toDBObject(t : E) : DBObject = {
            val bobj = toBObject(t)
            bobjectUpsertEncoder.toDBObject(bobj)
        }
    }

    override lazy val modifierEncoder = new ModifierEncoder[E]() with JavaDocumentEncoder[E] {
        override def toDBObject(t : E) : DBObject = {
            val bobj = toBObject(t)
            bobjectModifierEncoder.toDBObject(bobj)
        }
    }
}

object JavaBObjectBasedCodecs {
    def apply[E](to : (E) => BObject, from : (BObject) => E)(implicit bobjectQueryEncoder : QueryEncoder[BObject] with JavaDocumentEncoder[BObject],
        bobjectQueryResultDecoder : QueryResultDecoder[BObject] with JavaDocumentDecoder[BObject],
        bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject] with JavaDocumentEncoder[BObject],
        bobjectUpsertEncoder : UpsertEncoder[BObject] with JavaDocumentEncoder[BObject],
        bobjectModifierEncoder : ModifierEncoder[BObject] with JavaDocumentEncoder[BObject]) : JavaBObjectBasedCodecs[E] = {
        new JavaBObjectBasedCodecsImpl[E](to, from)
    }
}
