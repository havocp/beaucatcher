package org.beaucatcher.channel

import org.jboss.netty.buffer.ChannelBuffer
import java.nio.charset.Charset
import org.beaucatcher.wire.bson._
import org.beaucatcher.mongo._

package object netty {

    private[this] val zeroByte = 0.toByte
    private[this] val utf8Charset = Charset.forName("UTF-8");

    private[beaucatcher] def writeNulString(buf: ChannelBuffer, s: String): Unit = {
        val bytes = s.getBytes(utf8Charset)
        buf.ensureWritableBytes(bytes.length + 1)
        buf.writeBytes(bytes)
        buf.writeByte('\0')
    }

    private[beaucatcher] def writeLengthString(buf: ChannelBuffer, s: String): Unit = {
        val bytes = s.getBytes(utf8Charset)
        buf.ensureWritableBytes(bytes.length + 1)
        buf.writeInt(bytes.length + 1) // + 1 for nul
        buf.writeBytes(bytes)
        buf.writeByte('\0')
    }

    private[beaucatcher] def readNulString(buf: ChannelBuffer): String = {
        val start = buf.readerIndex
        val len = buf.bytesBefore(zeroByte)
        val bytes = new Array[Byte](len)
        buf.readBytes(bytes)
        buf.readByte() // mop up the nul
        new String(bytes, utf8Charset)
    }

    private[beaucatcher] def readLengthString(buf: ChannelBuffer): String = {
        val len = buf.readInt()
        val bytes = new Array[Byte](len - 1) // -1 for nul
        buf.readBytes(bytes)
        buf.readByte() // mop up the nul
        new String(bytes, utf8Charset)
    }

    private[beaucatcher] def skipNulString(buf: ChannelBuffer): Unit = {
        val len = buf.bytesBefore(zeroByte)
        buf.skipBytes(len + 1)
    }

    private[beaucatcher] def skipLengthString(buf: ChannelBuffer): Unit = {
        val len = buf.readInt()
        buf.skipBytes(len)
    }

    private[beaucatcher] def writeEmptyQuery(buf: ChannelBuffer): Unit = {
        buf.ensureWritableBytes(5)
        buf.writeInt(EMPTY_DOCUMENT_LENGTH)
        buf.writeByte(zeroByte)
    }

    private[beaucatcher] def writeQuery[Q](buf: ChannelBuffer, query: Q, maxSize: Int)(implicit querySupport: QueryEncodeSupport[Q]): Unit = {
        val start = buf.writerIndex()
        querySupport match {
            case nettySupport: NettyEncodeSupport[_] =>
                nettySupport.asInstanceOf[NettyEncodeSupport[Q]].write(buf, query)
            case _ =>
                val bb = querySupport.encode(query)
                buf.ensureWritableBytes(bb.remaining())
                buf.writeBytes(bb)
        }
        val size = buf.writerIndex() - start
        if (size > maxSize)
            throw new MongoException("Document is too large (" + size + " bytes but the max is " + maxSize + ")")
    }

    private[beaucatcher] def readEntity[E](buf: ChannelBuffer)(implicit entitySupport: EntityDecodeSupport[E]): E = {
        entitySupport match {
            case nettySupport: NettyDecodeSupport[_] =>
                nettySupport.asInstanceOf[NettyDecodeSupport[E]].read(buf)
            case _ =>
                entitySupport.decode(buf.toByteBuffer())
        }
    }
}
