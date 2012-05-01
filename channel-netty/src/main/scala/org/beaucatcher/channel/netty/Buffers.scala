package org.beaucatcher.channel.netty

import org.beaucatcher.mongo._
import org.jboss.netty.buffer.ChannelBuffer
import java.nio.ByteBuffer
import org.jboss.netty.buffer.ChannelBuffers

private[netty] final class Buffer private (private val buf: ChannelBuffer)
    extends EncodeBuffer with DecodeBuffer {

    def nettyBuf: ChannelBuffer = buf

    override def writerIndex: Int = buf.writerIndex

    override def ensureWritableBytes(writableBytes: Int): Unit = buf.ensureWritableBytes(writableBytes)

    // newer Netty seems to have writeBoolean but older doesn't
    override def writeBoolean(value: Boolean): Unit = buf.writeByte(if (value) 1 else 0)

    override def writeByte(value: Byte): Unit = buf.writeByte(value)

    override def writeInt(value: Int): Unit = buf.writeInt(value)

    override def writeLong(value: Long): Unit = buf.writeLong(value)

    override def writeDouble(value: Double): Unit = buf.writeDouble(value)

    override def writeBytes(src: Array[Byte]): Unit = buf.writeBytes(src)

    override def setInt(index: Int, value: Int): Unit = buf.setInt(index, value)

    override def writeBuffer(another: EncodeBuffer): Unit = {
        if (another eq this)
            throw new IllegalArgumentException("Attempt to write a buffer to itself")
        another match {
            case netty: Buffer =>
                buf.writeBytes(netty.nettyBuf, netty.nettyBuf.readerIndex, netty.nettyBuf.readableBytes())
            case _ =>
                buf.writeBytes(another.toByteBuffer())
        }
    }

    override def toByteBuffer(): ByteBuffer = {
        buf.toByteBuffer()
    }

    override def toDecodeBuffer(): DecodeBuffer = {
        // get a new readerIndex
        Buffer(ChannelBuffers.wrappedBuffer(nettyBuf))
    }

    override def writeBuffer(src: EncodeBuffer, srcIndex: Int, length: Int): Unit = {
        if (src eq this)
            throw new IllegalArgumentException("Attempt to write a buffer to itself")
        src match {
            case netty: Buffer =>
                netty.writeBuffer(netty, srcIndex, length)
            case _ =>
                buf.writeBytes(src.toByteBuffer())
        }
    }

    override def writeBytes(src: ByteBuffer): Unit = {
        buf.writeBytes(src)
    }

    override def readerIndex: Int = buf.readerIndex

    override def readableBytes: Int = buf.readableBytes

    // newer Netty seems to have readBoolean but older doesn't
    override def readBoolean(): Boolean = buf.readByte() != 0

    override def readByte(): Byte = buf.readByte()

    override def readInt(): Int = buf.readInt()

    override def readLong(): Long = buf.readLong()

    override def readDouble(): Double = buf.readDouble()

    override def readBytes(dst: Array[Byte]): Unit = buf.readBytes(dst)

    override def skipBytes(length: Int): Unit = buf.skipBytes(length)

    override def bytesBefore(value: Byte): Int = buf.bytesBefore(value)

    override def slice(index: Int, length: Int): DecodeBuffer = Buffer(buf.slice(index, length))
}

private[netty] object Buffer {
    def apply(buf: ChannelBuffer) = new Buffer(buf)
}
