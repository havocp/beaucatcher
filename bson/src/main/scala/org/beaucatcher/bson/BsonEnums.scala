package com.ometer.bson

import org.bson.BSON
import com.ometer.CaseEnum


private[bson] object BsonEnums {
    sealed trait JsonFlavor extends JsonFlavor.Value

    /**
     * See http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON
     * Flavors JS and TenGen can't be done using JsonAST from lift-json.
     *
     * Flavor CLEAN is made up and basically strips the type information
     * from STRICT, so just a string object ID instead of { "$oid" : <oid> }.
     * This is nicer in web services I think.
     */
    object JsonFlavor extends CaseEnum[JsonFlavor] {
        case object CLEAN extends JsonFlavor
        case object STRICT extends JsonFlavor
        case object JS extends JsonFlavor
        case object TEN_GEN extends JsonFlavor
        val values = List(CLEAN, STRICT, JS, TEN_GEN)
    }

    sealed trait BsonSubtype extends BsonSubtype.Value {
        val code : Byte
    }

    object BsonSubtype extends CaseEnum[BsonSubtype] {
        case object GENERAL extends BsonSubtype { override val code = BSON.B_GENERAL }
        case object FUNC extends BsonSubtype { override val code = BSON.B_FUNC }
        case object BINARY extends BsonSubtype { override val code = BSON.B_BINARY }
        case object UUID extends BsonSubtype { override val code = BSON.B_UUID }
        val values = List(GENERAL, FUNC, BINARY, UUID)

        def fromByte(b : Byte) : Option[BsonSubtype] = {
            values.find(_.code == b)
        }
    }

    sealed trait BsonType extends BsonType.Value {
        val code : Byte
    }

    object BsonType extends CaseEnum[BsonType] {
        /* These names match the ones in mongo-java-driver */
        case object EOO extends BsonType { override val code = BSON.EOO }
        case object NUMBER extends BsonType { override val code = BSON.NUMBER }
        case object STRING extends BsonType { override val code = BSON.STRING }
        case object OBJECT extends BsonType { override val code = BSON.OBJECT }
        case object ARRAY extends BsonType { override val code = BSON.ARRAY }
        case object BINARY extends BsonType { override val code = BSON.BINARY }
        case object UNDEFINED extends BsonType { override val code = BSON.UNDEFINED }
        case object OID extends BsonType { override val code = BSON.OID }
        case object BOOLEAN extends BsonType { override val code = BSON.BOOLEAN }
        case object DATE extends BsonType { override val code = BSON.DATE }
        case object NULL extends BsonType { override val code = BSON.NULL }
        case object REGEX extends BsonType { override val code = BSON.REGEX }
        case object REF extends BsonType { override val code = BSON.REF }
        case object CODE extends BsonType { override val code = BSON.CODE }
        case object SYMBOL extends BsonType { override val code = BSON.SYMBOL }
        case object CODE_W_SCOPE extends BsonType { override val code = BSON.CODE_W_SCOPE }
        case object NUMBER_INT extends BsonType { override val code = BSON.NUMBER_INT }
        case object TIMESTAMP extends BsonType { override val code = BSON.TIMESTAMP }
        case object NUMBER_LONG extends BsonType { override val code = BSON.NUMBER_LONG }
        case object MINKEY extends BsonType { override val code = BSON.MINKEY }
        case object MAXKEY extends BsonType { override val code = BSON.MAXKEY }

        val values = List(EOO, NUMBER, STRING, OBJECT, ARRAY, BINARY, UNDEFINED,
            OID, BOOLEAN, DATE, NULL, REGEX, REF, CODE, SYMBOL, CODE_W_SCOPE,
            TIMESTAMP, NUMBER_LONG, MINKEY, MAXKEY)

        def fromByte(b : Byte) : Option[BsonType] = {
            values.find(_.code == b)
        }
    }
}