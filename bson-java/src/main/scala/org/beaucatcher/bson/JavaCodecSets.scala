package org.beaucatcher.bson

import org.beaucatcher.mongo._

// this is a private object, it's accessed from `object Codecs`; it's separate
// so we can hide implicits we don't want
private[bson] object JavaCodecSets {

    def newBObjectCodecSet[IdType : IdEncoder]() : CollectionCodecSet[BObject, BObject, IdType, BValue] = {
        import JavaCodecs._
        CollectionCodecSet[BObject, BObject, IdType, BValue]()
    }

    def newCaseClassCodecSet[EntityType <: Product : Manifest, IdType : IdEncoder]() : CollectionCodecSet[BObject, EntityType, IdType, Any] = {
        // remove all ValueDecoder to avoid ambiguity
        import JavaCodecs.{ bvalueValueDecoder => _, _ }

        val entityCodecs = JavaCaseClassCodecs[EntityType]()
        import entityCodecs._

        // import the one true ValueDecoder
        implicit val valueDecoder = JavaCodecs.anyValueDecoder
        CollectionCodecSet[BObject, EntityType, IdType, Any]()
    }
}
