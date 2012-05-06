package org.beaucatcher.mongo

/** Mix this in to have some basic value decoders in scope. */
trait ValueDecoders {

    // TODO add type-specific decoders for each of the specific basic types
    // we support
}

/** Factory for value decoders */
object ValueDecoders {
    import CodecUtils._

    // this can't be implicit since it would always apply
    // and has the decoder parameter anyhow
    def anyValueDecoder[E]()(implicit documentDecoder: QueryResultDecoder[E]): ValueDecoder[Any] =
        new AnyValueDecoder[E]

    private class AnyValueDecoder[E](implicit val documentDecoder: QueryResultDecoder[E])
        extends ValueDecoder[Any] {
        override def decode(code: Byte, buf: DecodeBuffer): Any = {
            readAny[E](code, buf)
        }
    }
}
