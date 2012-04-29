package org.beaucatcher.channel.netty

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.beaucatcher.channel._
import org.beaucatcher.mongo._

/** Encoder optimized for netty */
trait NettyDocumentEncoder[-T] extends DocumentEncoder[T] {
    override final def encode(t: T): ByteBuffer = {
        val buf = ChannelBuffers.buffer(ByteOrder.LITTLE_ENDIAN, 128)
        write(buf, t)
        buf.toByteBuffer()
    }

    def write(buf: ChannelBuffer, t: T): Unit
}

/** Decoder optimized for netty */
trait NettyDocumentDecoder[+T] extends DocumentDecoder[T] {
    override final def decode(buf: ByteBuffer): T = {
        if (buf.order() != ByteOrder.LITTLE_ENDIAN)
            throw new MongoException("ByteBuffer to decode must be little endian")
        val nettyBuf = ChannelBuffers.wrappedBuffer(buf)
        read(nettyBuf)
    }

    def read(buf: ChannelBuffer): T
}
