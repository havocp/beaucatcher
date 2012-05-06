package org.beaucatcher.bson

import org.beaucatcher.wire._

/** A detailed type tag for binary data in Bson. */
object BsonSubtype extends Enumeration {
    type BsonSubtype = Value
    val GENERAL, FUNC, BINARY, UUID, MD5, USER_DEFINED = Value

    private val fromBytes =
        Map(Bson.B_GENERAL -> GENERAL,
            Bson.B_FUNC -> FUNC,
            Bson.B_BINARY -> BINARY,
            Bson.B_UUID -> UUID,
            Bson.B_MD5 -> MD5,
            Bson.B_USER_DEFINED -> USER_DEFINED)

    private val toBytes = fromBytes map { _.swap }

    def fromByte(b: Byte): Option[BsonSubtype] = {
        fromBytes.get(b)
    }

    def toByte(v: Value): Byte = {
        toBytes.getOrElse(v, throw new IllegalArgumentException("bad BsonSubtype value"))
    }
}
