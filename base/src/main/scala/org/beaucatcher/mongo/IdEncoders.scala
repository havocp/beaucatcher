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
        override def encodeFieldAny(value: String): Any = value
    }

    private object ObjectIdIdEncoder
        extends IdEncoder[ObjectId] {
        override def encodeField(buf: EncodeBuffer, name: String, value: ObjectId): Unit = {
            writeFieldObjectId(buf, name, value)
        }
        override def encodeFieldAny(value: ObjectId): Any = value
    }

    private object IntIdEncoder
        extends IdEncoder[Int] {
        override def encodeField(buf: EncodeBuffer, name: String, value: Int): Unit = {
            writeFieldInt(buf, name, value)
        }
        override def encodeFieldAny(value: Int): Any = value
    }

    private object LongIdEncoder
        extends IdEncoder[Long] {
        override def encodeField(buf: EncodeBuffer, name: String, value: Long): Unit = {
            writeFieldLong(buf, name, value)
        }
        override def encodeFieldAny(value: Long): Any = value
    }

}

/** Mixin trait for CollectionCodecSet subclasses, providing an encoder for String ID. */
trait CollectionCodecSetIdEncoderString extends CollectionCodecSetIdEncoder[String] {
    self: CollectionCodecSet[_, _, _, String, _] =>

    override implicit def collectionIdEncoder = IdEncoders.stringIdEncoder
}

/** Mixin trait for CollectionCodecSet subclasses, providing an encoder for ObjectId ID. */
trait CollectionCodecSetIdEncoderObjectId extends CollectionCodecSetIdEncoder[ObjectId] {
    self: CollectionCodecSet[_, _, _, ObjectId, _] =>

    override implicit def collectionIdEncoder = IdEncoders.objectIdIdEncoder
}

/** Mixin trait for CollectionCodecSet subclasses, providing an encoder for Int ID. */
trait CollectionCodecSetIdEncoderInt extends CollectionCodecSetIdEncoder[Int] {
    self: CollectionCodecSet[_, _, _, Int, _] =>

    override implicit def collectionIdEncoder = IdEncoders.intIdEncoder
}

/** Mixin trait for CollectionCodecSet subclasses, providing an encoder for Long ID. */
trait CollectionCodecSetIdEncoderLong extends CollectionCodecSetIdEncoder[Long] {
    self: CollectionCodecSet[_, _, _, Long, _] =>

    override implicit def collectionIdEncoder = IdEncoders.longIdEncoder
}
