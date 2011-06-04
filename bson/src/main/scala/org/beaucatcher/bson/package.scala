package org.beaucatcher

package object bson {
    // Enumeration vals must be first so they
    // are initialized before BsonAST tries to use
    // them
    val BsonSubtype = BsonEnums.BsonSubtype
    val BsonType = BsonEnums.BsonType
    val JsonFlavor = BsonEnums.JsonFlavor

    type BArray = BsonAST.BArray
    val BArray = BsonAST.BArray

    type BBinData = BsonAST.BBinData
    val BBinData = BsonAST.BBinData

    type BBoolean = BsonAST.BBoolean
    val BBoolean = BsonAST.BBoolean

    type BDouble = BsonAST.BDouble
    val BDouble = BsonAST.BDouble

    type BInt32 = BsonAST.BInt32
    val BInt32 = BsonAST.BInt32

    type BInt64 = BsonAST.BInt64
    val BInt64 = BsonAST.BInt64

    type BISODate = BsonAST.BISODate
    val BISODate = BsonAST.BISODate

    val BNull = BsonAST.BNull

    type BObject = BsonAST.BObject
    val BObject = BsonAST.BObject

    type BObjectId = BsonAST.BObjectId
    val BObjectId = BsonAST.BObjectId

    type BString = BsonAST.BString
    val BString = BsonAST.BString

    type BTimestamp = BsonAST.BTimestamp
    val BTimestamp = BsonAST.BTimestamp

    type BValue = BsonAST.BValue
    val BValue = BsonAST.BValue

    type JArray = BsonAST.JArray
    val JArray = BsonAST.JArray

    type JObject = BsonAST.JObject
    val JObject = BsonAST.JObject

    type JValue = BsonAST.JValue
    val JValue = BsonAST.JValue
}
