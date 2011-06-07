package org.beaucatcher.bson

import org.bson.BSON

/**
 * See http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON
 * Flavors JS and TenGen can't be done using JsonAST from lift-json.
 *
 * Flavor CLEAN is made up and basically strips the type information
 * from STRICT, so just a string object ID instead of { "$oid" : <oid> }.
 * This is nicer in web services I think.
 */
object JsonFlavor extends Enumeration {
    type JsonFlavor = Value
    val CLEAN, STRICT, JS, TEN_GEN = Value
}

object BsonSubtype extends Enumeration {
    type BsonSubtype = Value
    val GENERAL, FUNC, BINARY, UUID = Value

    private val fromBytes =
        Map(BSON.B_GENERAL -> GENERAL,
            BSON.B_FUNC -> FUNC,
            BSON.B_BINARY -> BINARY,
            BSON.B_UUID -> UUID)

    private val toBytes = fromBytes map { _.swap }

    def fromByte(b : Byte) : Option[BsonSubtype] = {
        fromBytes.get(b)
    }

    def toByte(v : Value) : Byte = {
        toBytes.getOrElse(v, throw new IllegalArgumentException("bad BsonSubtype value"))
    }
}

object BsonType extends Enumeration {
    type BsonType = Value

    /* These names match the ones in mongo-java-driver */
    val EOO, NUMBER, STRING, OBJECT, ARRAY, BINARY, UNDEFINED, OID, BOOLEAN, DATE, NULL, REGEX, REF, CODE, SYMBOL, CODE_W_SCOPE, NUMBER_INT, TIMESTAMP, NUMBER_LONG, MINKEY, MAXKEY = Value

    private val fromBytes =
        Map(BSON.EOO -> EOO,
            BSON.NUMBER -> NUMBER,
            BSON.STRING -> STRING,
            BSON.OBJECT -> OBJECT,
            BSON.ARRAY -> ARRAY,
            BSON.BINARY -> BINARY,
            BSON.UNDEFINED -> UNDEFINED,
            BSON.OID -> OID,
            BSON.BOOLEAN -> BOOLEAN,
            BSON.DATE -> DATE,
            BSON.NULL -> NULL,
            BSON.REGEX -> REGEX,
            BSON.REF -> REF,
            BSON.CODE -> CODE,
            BSON.SYMBOL -> SYMBOL,
            BSON.CODE_W_SCOPE -> CODE_W_SCOPE,
            BSON.NUMBER_INT -> NUMBER_INT,
            BSON.TIMESTAMP -> TIMESTAMP,
            BSON.NUMBER_LONG -> NUMBER_LONG,
            BSON.MINKEY -> MINKEY,
            BSON.MAXKEY -> MAXKEY)

    private val toBytes = fromBytes map { _.swap }

    def fromByte(b : Byte) : Option[BsonType] = {
        fromBytes.get(b)
    }

    def toByte(v : Value) : Byte = {
        toBytes.getOrElse(v, throw new IllegalArgumentException("bad BsonType value"))
    }
}

