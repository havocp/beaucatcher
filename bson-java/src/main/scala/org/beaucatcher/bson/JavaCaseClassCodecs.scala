package org.beaucatcher.bson

import org.beaucatcher.mongo._

final class JavaCaseClassCodecs[E <: AnyRef with Product] private ()(implicit manifestE : Manifest[E],
    bobjectQueryEncoder : QueryEncoder[BObject] with JavaDocumentEncoder[BObject],
    bobjectQueryResultDecoder : QueryResultDecoder[BObject] with JavaDocumentDecoder[BObject],
    bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject] with JavaDocumentEncoder[BObject],
    bobjectUpsertEncoder : UpsertEncoder[BObject] with JavaDocumentEncoder[BObject],
    bobjectModifierEncoder : ModifierEncoder[BObject] with JavaDocumentEncoder[BObject]) {

    private lazy val analysis : ClassAnalysis[E] = {
        new ClassAnalysis[E](manifestE.erasure.asInstanceOf[Class[E]])
    }

    // TODO don't create intermediate BObject
    private lazy val codecs = JavaBObjectBasedCodecs[E]({ o =>
        BObject(analysis.asMap(o).map(kv => (kv._1, BValue.wrap(kv._2))))
    }, { o =>
        analysis.fromMap(o.unwrapped)
    })

    implicit def queryResultDecoder : QueryResultDecoder[E] with JavaDocumentDecoder[E] = codecs.queryResultDecoder

    implicit def updateQueryEncoder : UpdateQueryEncoder[E] with JavaDocumentEncoder[E] = codecs.updateQueryEncoder

    implicit def upsertEncoder : UpsertEncoder[E] with JavaDocumentEncoder[E] = codecs.upsertEncoder

    implicit def modifierEncoder : ModifierEncoder[E] with JavaDocumentEncoder[E] = codecs.modifierEncoder
}

object JavaCaseClassCodecs {
    /**
     * Create a set of codecs for a case class, with opaque implementation.
     * (this method should change to not go via BObject).
     */
    def apply[E <: Product : Manifest]() : JavaCaseClassCodecs[E] = {
        import JavaCodecs._
        new JavaCaseClassCodecs[E]
    }
}
