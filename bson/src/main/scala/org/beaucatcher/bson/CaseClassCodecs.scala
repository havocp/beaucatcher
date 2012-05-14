package org.beaucatcher.bson

import org.beaucatcher.mongo._

/**
 * Encoders/Decoders for case classes; this is expensive to
 * create so should be cached. We don't let you use a case
 * class for a query since the intent is unclear (just _id or
 * all fields?).
 */
final class CaseClassCodecs[E <: AnyRef with Product] private ()(implicit manifestE: Manifest[E])
    extends IdEncoders with ValueDecoders {

    private lazy val analysis: ClassAnalysis[E] = {
        new ClassAnalysis[E](manifestE.erasure.asInstanceOf[Class[E]])
    }

    private lazy val codecs = new IteratorBasedCodecs[E]() {
        private def fromAny(x: Any): Any = {
            x match {
                case null =>
                    null
                case i: Iterator[_] =>
                    MapCodecs.iteratorToMap(i.asInstanceOf[Iterator[(String, Any)]])
                case other =>
                    other
            }
        }

        override def toIterator(o: E): Iterator[(String, Any)] = {
            val fields = analysis.fieldIterator(o)
            // remove Option from optional fields and convert maps
            // to iterators
            fields.filter(_._2 != None).map({
                // null first since it would match our other typechecks
                case (name, Some(x)) => (name, fromAny(x))
                case (name, x) => (name, fromAny(x))
            })
        }

        override def fromIterator(i: Iterator[(String, Any)]): E = {
            analysis.fromMap(MapCodecs.iteratorToMap(i))
        }
    }

    // can't be implicit since it would always apply
    // we also offer "caseClassValueDecoder" that
    // will decode docs as BObject, so here we
    // decode them as plain maps.
    def anyValueDecoder: ValueDecoder[Any] =
        MapCodecs.anyValueDecoder

    /**
     * The query encoder is not implicit by default because usually
     * it doesn't make sense to use the case class for a query
     */
    def queryEncoder: QueryEncoder[E] = codecs.queryEncoder

    implicit def queryResultDecoder: QueryResultDecoder[E] = codecs.queryResultDecoder

    implicit def updateQueryEncoder: UpdateQueryEncoder[E] = codecs.updateQueryEncoder

    implicit def upsertEncoder: UpsertEncoder[E] = codecs.upsertEncoder

    implicit def modifierEncoder: ModifierEncoder[E] = codecs.modifierEncoder

    def newCodecSet[EntityType <: Product: Manifest, IdType: IdEncoder](): CollectionCodecSet[BObject, EntityType, EntityType, IdType, Any] =
        CodecSets.newCaseClassCodecSet()
}

object CaseClassCodecs {
    /**
     * Create a set of codecs for a case class, with opaque implementation.
     */
    def apply[E <: Product: Manifest](): CaseClassCodecs[E] = {
        new CaseClassCodecs[E]
    }
}
