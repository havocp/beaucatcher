package org.beaucatcher.mongo

/** Mix this in to have some basic value decoders in scope. */
trait ValueDecoders {

    // it isn't safe to make this implicit since it would always apply
    def anyValueDecoder: ValueDecoder[Any] =
        ValueDecoders.anyValueDecoder

    // TODO add type-specific decoders for each of the specific basic types
    // we support
}

/** Import from this to have some basic value decoders in scope. */
object ValueDecoders extends ValueDecoders {
    import CodecUtils._

    // it isn't safe to make this implicit since it would always apply
    override def anyValueDecoder: ValueDecoder[Any] =
        AnyValueDecoder

    private object AnyValueDecoder
        extends ValueDecoder[Any] {
        override def decode(code: Byte, buf: DecodeBuffer): Any = {
            import MapCodecs._
            readAny[Map[String, Any]](code, buf)
        }
    }
}
