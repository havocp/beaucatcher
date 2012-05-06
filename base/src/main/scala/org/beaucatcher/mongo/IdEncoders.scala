package org.beaucatcher.mongo

import org.beaucatcher.bson._

/** Mix this trait in to have some basic IdEncoder in scope. */
trait IdEncoders {

    implicit def stringIdEncoder: IdEncoder[String] =
        IdEncoders.stringIdEncoder

    implicit def intIdEncoder: IdEncoder[Int] =
        IdEncoders.intIdEncoder

    implicit def longIdEncoder: IdEncoder[Long] =
        IdEncoders.longIdEncoder

    implicit def objectIdIdEncoder: IdEncoder[ObjectId] =
        IdEncoders.objectIdIdEncoder

}

/**
 * Import this trait to have some basic IdEncoder in scope.
 * Most other objects that export a group of codecs also export this.
 */
object IdEncoders extends IdEncoders {
    import CodecUtils._

    override implicit def stringIdEncoder: IdEncoder[String] =
        StringIdEncoder

    override implicit def intIdEncoder: IdEncoder[Int] =
        IntIdEncoder

    override implicit def longIdEncoder: IdEncoder[Long] =
        LongIdEncoder

    override implicit def objectIdIdEncoder: IdEncoder[ObjectId] =
        ObjectIdIdEncoder

    private object StringIdEncoder
        extends IdEncoder[String] {
        override def encodeField(buf: EncodeBuffer, name: String, value: String): Unit = {
            writeFieldString(buf, name, value)
        }
    }

    private object ObjectIdIdEncoder
        extends IdEncoder[ObjectId] {
        override def encodeField(buf: EncodeBuffer, name: String, value: ObjectId): Unit = {
            writeFieldObjectId(buf, name, value)
        }
    }

    private object IntIdEncoder
        extends IdEncoder[Int] {
        override def encodeField(buf: EncodeBuffer, name: String, value: Int): Unit = {
            writeFieldInt(buf, name, value)
        }
    }

    private object LongIdEncoder
        extends IdEncoder[Long] {
        override def encodeField(buf: EncodeBuffer, name: String, value: Long): Unit = {
            writeFieldLong(buf, name, value)
        }
    }

}