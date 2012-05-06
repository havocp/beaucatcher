package org.beaucatcher.bson

import org.beaucatcher.mongo._

// this is a private object, it's accessed from `object Codecs`; it's separate
// so we can hide implicits we don't want
private[bson] object CodecSets {

    def newBObjectCodecSet[IdType : IdEncoder]() : CollectionCodecSet[BObject, BObject, IdType, BValue] = {
        import BObjectCodecs._
        CollectionCodecSet[BObject, BObject, IdType, BValue]()
    }

    def newCaseClassCodecSet[EntityType <: Product : Manifest, IdType : IdEncoder]() : CollectionCodecSet[BObject, EntityType, IdType, Any] = {
        // remove all ValueDecoder to avoid ambiguity
        import BObjectCodecs.{ bvalueValueDecoder => _, _ }

        // TODO change back to just apply()
        val entityCodecs = CaseClassCodecs[EntityType]()
        import entityCodecs._

        // import the one true ValueDecoder
        implicit val valueDecoder = BObjectCodecs.anyValueDecoder
        CollectionCodecSet[BObject, EntityType, IdType, Any]()
    }
}
