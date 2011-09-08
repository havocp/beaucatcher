package org.beaucatcher.bson.wire

object `package` {
    val EOO : Byte = 0
    val NUMBER : Byte = 1
    val STRING : Byte = 2
    val OBJECT : Byte = 3
    val ARRAY : Byte = 4
    val BINARY : Byte = 5
    val UNDEFINED : Byte = 6
    val OID : Byte = 7
    val BOOLEAN : Byte = 8
    val DATE : Byte = 9
    val NULL : Byte = 10
    val REGEX : Byte = 11
    val REF : Byte = 12
    val CODE : Byte = 13
    val SYMBOL : Byte = 14
    val CODE_W_SCOPE : Byte = 15
    val NUMBER_INT : Byte = 16
    val TIMESTAMP : Byte = 17
    val NUMBER_LONG : Byte = 18

    val MINKEY : Byte = -1
    val MAXKEY : Byte = 127

    val B_GENERAL : Byte = 0
    val B_FUNC : Byte = 1
    val B_BINARY : Byte = 2
    val B_UUID : Byte = 3
}
