package org.beaucatcher.bson

import org.beaucatcher.mongo._
import com.mongodb.DBObject
import com.mongodb.DefaultDBDecoder
import com.mongodb.DefaultDBEncoder
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.bson.BSONDecoder
import org.bson.BSONEncoder
import org.bson.BSONObject

// TODO this whole concept of Java-driver-specific encoders is one big
// terrible hack. But using the Java driver as a backend is kind of
// a hack to begin with, and hopefully isn't a long-term situation.

/** DocumentEncoder optimized for mongo-java-driver */
trait JavaDocumentEncoder[-T] extends DocumentEncoder[T] {
    // the "lose" case which we'd probably never use
    // (should only happen if used with another driver)
    override def encode(buf : EncodeBuffer, t : T) : Unit = {
        val bb = JavaCodecs.encode(toBsonObject(t))
        buf.writeBytes(bb)
    }

    def toBsonObject(t : T) : BSONObject = toDBObject(t)

    def toDBObject(t : T) : DBObject
}

/** DocumentDecoder optimized for mongo-java-driver */
trait JavaDocumentDecoder[+T] extends DocumentDecoder[T] {
    // the "lose" case which we'd probably never use
    // (should only happen if used with another driver)
    override def decode(buf : DecodeBuffer) : T = {
        fromBsonObject(JavaCodecs.decode(buf.toByteBuffer()))
    }

    def fromBsonObject(obj : BSONObject) : T
}

/** ValueDecoder that supports mongo-java-driver */
trait JavaValueDecoder[+T] extends ValueDecoder[T] {
    override def decode(code : Byte, buf : DecodeBuffer) : T = {
        throw new MongoException("this ValueDecoder only works with the mongo-java-driver backend " + this)
    }

    def convert(valueFromBSONObject : Any) : T
}

/** IdEncoder that supports mongo-java-driver */
trait JavaIdEncoder[-T] extends IdEncoder[T] {
    override def encodeField(buf : EncodeBuffer, name : String, t : T) : Unit = {
        throw new MongoException("this IdEncoder only works with the mongo-java-driver backend " + this)
    }

    def put(obj : BSONObject, name : String, t : T) : Unit
}

private[beaucatcher] object JavaCodecs {
    implicit def stringIdEncoder : IdEncoder[String] with JavaIdEncoder[String] =
        AnyIdEncoder

    implicit def intIdEncoder : IdEncoder[Int] with JavaIdEncoder[Int] =
        AnyIdEncoder

    implicit def longIdEncoder : IdEncoder[Long] with JavaIdEncoder[Long] =
        AnyIdEncoder

    implicit def objectIdIdEncoder : IdEncoder[ObjectId] with JavaIdEncoder[ObjectId] =
        ObjectIdIdEncoder

    implicit def bvalueValueDecoder : ValueDecoder[BValue] with JavaValueDecoder[BValue] =
        BValueValueDecoder

    // it isn't safe to make this implicit since it would always apply
    def anyValueDecoder : ValueDecoder[Any] =
        AnyValueDecoder

    implicit def bobjectQueryEncoder : QueryEncoder[BObject] with JavaDocumentEncoder[BObject] =
        BObjectUnmodifiedEncoder

    implicit def bobjectQueryResultDecoder : QueryResultDecoder[BObject] with JavaDocumentDecoder[BObject] =
        BObjectDecoder

    implicit def bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject] with JavaDocumentEncoder[BObject] =
        BObjectOnlyIdEncoder

    implicit def bobjectModifierEncoder : ModifierEncoder[BObject] with JavaDocumentEncoder[BObject] =
        BObjectWithoutIdEncoder

    implicit def bobjectUpsertEncoder : UpsertEncoder[BObject] with JavaDocumentEncoder[BObject] =
        BObjectUnmodifiedEncoder

    def newBObjectCodecSet[IdType : IdEncoder]() : CollectionCodecSet[BObject, BObject, IdType, BValue] =
        JavaCodecSets.newBObjectCodecSet()

    def newCaseClassCodecSet[EntityType <: Product : Manifest, IdType : IdEncoder]() : CollectionCodecSet[BObject, EntityType, IdType, Any] =
        JavaCodecSets.newCaseClassCodecSet()

    private[beaucatcher] object BObjectUnmodifiedEncoder
        extends JavaDocumentEncoder[BObject]
        with QueryEncoder[BObject]
        with UpsertEncoder[BObject] {
        override def toDBObject(o : BObject) : DBObject = {
            new JavaConversions.BObjectDBObject(o)
        }
    }

    private[beaucatcher] object BObjectWithoutIdEncoder
        extends ModifierEncoder[BObject]
        with JavaDocumentEncoder[BObject] {
        override def toDBObject(o : BObject) : DBObject = {
            BObjectUnmodifiedEncoder.toDBObject(o - "_id")
        }
    }

    // this is an object encoder that only encodes the ID,
    // not an IdEncoder
    private[beaucatcher] object BObjectOnlyIdEncoder
        extends UpdateQueryEncoder[BObject]
        with JavaDocumentEncoder[BObject] {
        override def toDBObject(o : BObject) : DBObject = {
            import JavaConversions._
            val id = o.getOrElse("_id", throw new BugInSomethingMongoException("only objects with an _id field work here")).unwrappedAsJava
            val obj = new com.mongodb.BasicDBObject()
            obj.put("_id", id)
            obj
        }
    }

    private[beaucatcher] object AnyIdEncoder
        extends IdEncoder[Any]
        with JavaIdEncoder[Any] {
        override def put(obj : BSONObject, name : String, value : Any) : Unit = {
            obj.put(name, value)
        }
    }

    private[beaucatcher] object ObjectIdIdEncoder
        extends IdEncoder[ObjectId]
        with JavaIdEncoder[ObjectId] {
        override def put(obj : BSONObject, name : String, value : ObjectId) : Unit = {
            obj.put(name, JavaConversions.asJavaObjectId(value))
        }
    }

    private[beaucatcher] object BValueValueDecoder
        extends ValueDecoder[BValue]
        with JavaValueDecoder[BValue] {
        override def convert(valueFromBSONObject : Any) : BValue = {
            JavaConversions.wrapJavaAsBValue(valueFromBSONObject)
        }
    }

    private[beaucatcher] object AnyValueDecoder
        extends JavaValueDecoder[Any]
        with ValueDecoder[Any] {

        implicit val errorDecoder = new QueryResultDecoder[Unit] {
            override def decode(buf : DecodeBuffer) : Unit = {
                throw new MongoException("Cannot decode an object value here because it's unknown how to represent the object in Scala")
            }
        }

        override def convert(valueFromBSONObject : Any) : Any = {
            import JavaConversions._
            // convert to BValue then unwrap to Any
            wrapJavaAsBValue(valueFromBSONObject).unwrapped
        }
    }

    private[beaucatcher] object BObjectDecoder
        extends JavaDocumentDecoder[BObject]
        with QueryResultDecoder[BObject] {
        override def fromBsonObject(obj : BSONObject) : BObject = {
            import JavaConversions._
            obj
        }
    }

    private lazy val encoders = new ThreadLocal[BSONEncoder]() {
        override def initialValue() : BSONEncoder = {
            new DefaultDBEncoder()
        }
    }

    private lazy val decoders = new ThreadLocal[BSONDecoder]() {
        override def initialValue() : BSONDecoder = {
            new DefaultDBDecoder()
        }
    }

    private[bson] def encode(obj : BSONObject) : ByteBuffer = {
        val encoder = JavaCodecs.encoders.get
        ByteBuffer.wrap(encoder.encode(obj))
    }

    private[bson] def decode(buf : ByteBuffer) : BSONObject = {
        if (buf.order() != ByteOrder.LITTLE_ENDIAN)
            throw new MongoException("ByteBuffer to decode must be little endian")

        val array = if (buf.hasArray) {
            buf.array
        } else {
            // surely there's a better way
            val copy = ByteBuffer.allocate(buf.capacity())
            copy.mark()
            copy.put(buf)
            copy.reset()
            require(copy.hasArray) // ByteBuffer.allocate guarantees this
            copy.array
        }

        val decoder = JavaCodecs.decoders.get

        val obj = decoder.readObject(array)
        obj
    }
}
