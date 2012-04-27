package org.beaucatcher.jdriver

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.beaucatcher.mongo._
import org.bson.BSONObject
import com.mongodb.DefaultDBDecoder
import com.mongodb.DefaultDBEncoder
import org.bson.BSONEncoder
import org.bson.BSONDecoder
import com.mongodb.DBObject

/** EncodeSupport optimized for mongo-java-driver */
trait JavaEncodeSupport[-T] extends EncodeSupport[T] {
    // the "lose" case which we'd probably never use
    // (should only happen if used with another driver)
    override final def encode(t : T) : ByteBuffer = {
        JavaSupport.encode(toBsonObject(t))
    }

    def toBsonObject(t : T) : BSONObject = toDBObject(t)

    def toDBObject(t : T) : DBObject
}

/** DecodeSupport optimized for mongo-java-driver */
trait JavaDecodeSupport[+T] extends DecodeSupport[T] {
    // the "lose" case which we'd probably never use
    // (should only happen if used with another driver)
    override final def decode(buf : ByteBuffer) : T = {
        fromBsonObject(JavaSupport.decode(buf))
    }

    def fromBsonObject(obj : BSONObject) : T
}

private[jdriver] object JavaSupport {
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

    def encode(obj : BSONObject) : ByteBuffer = {
        val encoder = JavaSupport.encoders.get
        ByteBuffer.wrap(encoder.encode(obj))
    }

    def decode(buf : ByteBuffer) : BSONObject = {
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

        val decoder = JavaSupport.decoders.get

        val obj = decoder.readObject(array)
        obj
    }
}
