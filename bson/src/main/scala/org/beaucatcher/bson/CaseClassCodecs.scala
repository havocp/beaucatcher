package org.beaucatcher.bson

import org.beaucatcher.mongo._

/**
 * Encoders/Decoders for case classes; this is expensive to
 * create so should be cached. We don't let you use a case
 * class for a query since the intent is unclear (just _id or
 * all fields?).
 */
final class CaseClassCodecs[E <: AnyRef with Product] private ()(implicit manifestE : Manifest[E], bobjectQueryEncoder : QueryEncoder[BObject],
    bobjectQueryResultDecoder : QueryResultDecoder[BObject],
    bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject],
    bobjectUpsertEncoder : UpsertEncoder[BObject],
    bobjectModifierEncoder : ModifierEncoder[BObject]) {

    private lazy val analysis : ClassAnalysis[E] = {
        new ClassAnalysis[E](manifestE.erasure.asInstanceOf[Class[E]])
    }

    // TODO don't create intermediate BObject
    private lazy val codecs = BObjectBasedCodecs[E]({ o =>
        BObject(analysis.asMap(o).map(kv => (kv._1, BValue.wrap(kv._2))))
    }, { o =>
        analysis.fromMap(o.unwrapped)
    })

    implicit def queryResultDecoder : QueryResultDecoder[E] = codecs.queryResultDecoder

    implicit def updateQueryEncoder : UpdateQueryEncoder[E] = codecs.updateQueryEncoder

    implicit def upsertEncoder : UpsertEncoder[E] = codecs.upsertEncoder

    implicit def modifierEncoder : ModifierEncoder[E] = codecs.modifierEncoder
}

object CaseClassCodecs {
    /**
     * Create a set of codecs for a case class, with opaque implementation.
     * (this method should change to not go via BObject).
     */
    def apply[E <: Product : Manifest]() : CaseClassCodecs[E] = {
        import Codecs._
        new CaseClassCodecs[E]
    }
}
