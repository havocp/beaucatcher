package org.beaucatcher.bson

import org.beaucatcher.mongo._

/**
 * Generate encoders/decoders based on converting an object to/from
 * BObject, as an alternative to writing to/from the BSON byte stream.
 * This is a lot easier to implement, but has some performance cost.
 */
trait BObjectBasedCodecs[E] {
    implicit def queryEncoder : QueryEncoder[E]

    implicit def queryResultDecoder : QueryResultDecoder[E]

    implicit def updateQueryEncoder : UpdateQueryEncoder[E]

    implicit def upsertEncoder : UpsertEncoder[E]

    implicit def modifierEncoder : ModifierEncoder[E]
}

private final class BObjectBasedCodecsImpl[E](toBObject : (E) => BObject,
    fromBObject : (BObject) => E)(implicit bobjectQueryEncoder : QueryEncoder[BObject],
        bobjectQueryResultDecoder : QueryResultDecoder[BObject],
        bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject],
        bobjectUpsertEncoder : UpsertEncoder[BObject],
        bobjectModifierEncoder : ModifierEncoder[BObject]) extends BObjectBasedCodecs[E] {

    override lazy val queryEncoder = new QueryEncoder[E]() {
        override def encode(buf : EncodeBuffer, o : E) : Unit = {
            bobjectQueryEncoder.encode(buf, toBObject(o))
        }
    }

    override lazy val queryResultDecoder = new QueryResultDecoder[E]() {
        override def decode(buf : DecodeBuffer) : E = {
            val o = bobjectQueryResultDecoder.decode(buf)
            fromBObject(o)
        }
    }

    override lazy val updateQueryEncoder = new UpdateQueryEncoder[E]() {
        override def encode(buf : EncodeBuffer, o : E) : Unit = {
            bobjectUpdateQueryEncoder.encode(buf, toBObject(o))
        }
    }

    override lazy val upsertEncoder = new UpsertEncoder[E]() {
        override def encode(buf : EncodeBuffer, o : E) : Unit = {
            bobjectUpsertEncoder.encode(buf, toBObject(o))
        }
    }

    override lazy val modifierEncoder = new ModifierEncoder[E]() {
        override def encode(buf : EncodeBuffer, o : E) : Unit = {
            bobjectModifierEncoder.encode(buf, toBObject(o))
        }
    }
}

object BObjectBasedCodecs {
    def apply[E](to : (E) => BObject, from : (BObject) => E)(implicit bobjectQueryEncoder : QueryEncoder[BObject],
        bobjectQueryResultDecoder : QueryResultDecoder[BObject],
        bobjectUpdateQueryEncoder : UpdateQueryEncoder[BObject],
        bobjectUpsertEncoder : UpsertEncoder[BObject],
        bobjectModifierEncoder : ModifierEncoder[BObject]) : BObjectBasedCodecs[E] = {
        new BObjectBasedCodecsImpl[E](to, from)
    }
}
