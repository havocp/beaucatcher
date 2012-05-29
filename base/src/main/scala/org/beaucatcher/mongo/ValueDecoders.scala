package org.beaucatcher.mongo

/** Mix this in to have some basic value decoders in scope. */
trait ValueDecoders {

    // this can't be implicit since it would always apply
    // and has the decoder parameter anyhow
    def anyValueDecoder[E]()(implicit documentDecoder: QueryResultDecoder[E]): ValueDecoder[Any] =
        ValueDecoders.anyValueDecoder()

    // TODO add type-specific decoders for each of the specific basic types
    // we support
}

/** Factory for value decoders */
object ValueDecoders extends ValueDecoders {
    import CodecUtils._

    // this can't be implicit since it would always apply
    // and has the decoder parameter anyhow
    override def anyValueDecoder[E]()(implicit documentDecoder: QueryResultDecoder[E]): ValueDecoder[Any] =
        new AnyValueDecoder[E]

    private class AnyValueDecoder[E](implicit val documentDecoder: QueryResultDecoder[E])
        extends ValueDecoder[Any] {
        override def decode(code: Byte, buf: DecodeBuffer): Any = {
            readAny[E](code, buf)
        }
        override def decodeAny(value: Any): Any = {
            value match {
                case i: Iterator[(String, Any)] =>
                    documentDecoder.decodeIterator(i)
                case x =>
                    x
            }
        }
    }
}

trait CollectionCodecSetValueDecoderAny[+NestedDocumentType] extends CollectionCodecSetValueDecoder[Any] {
    self: CollectionCodecSet[_, _, _, _, Any] =>

    protected implicit def nestedDocumentQueryResultDecoder: QueryResultDecoder[NestedDocumentType]

    override implicit def collectionValueDecoder: ValueDecoder[Any] =
        ValueDecoders.anyValueDecoder[NestedDocumentType]
}
