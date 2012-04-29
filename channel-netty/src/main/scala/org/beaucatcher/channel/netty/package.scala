package org.beaucatcher.channel

import org.jboss.netty.buffer.ChannelBuffer
import java.nio.charset.Charset
import org.beaucatcher.wire._
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

    private[beaucatcher] def skipDocument(buf: ChannelBuffer): Unit = {
        val len = buf.readInt()
        buf.skipBytes(len - 4)
    }

    private[beaucatcher] def writeEmptyQuery(buf: ChannelBuffer): Unit = {
        buf.ensureWritableBytes(5)
        buf.writeInt(Bson.EMPTY_DOCUMENT_LENGTH)
        buf.writeByte(zeroByte)
    }

    private[beaucatcher] def writeDocument[D](buf: ChannelBuffer, doc: D, maxSize: Int)(implicit encodeSupport: DocumentEncoder[D]): Unit = {
        val start = buf.writerIndex()
        encodeSupport match {
            case nettySupport: NettyDocumentEncoder[_] =>
                nettySupport.asInstanceOf[NettyDocumentEncoder[D]].write(buf, doc)
            case _ =>
                val bb = encodeSupport.encode(doc)
                buf.ensureWritableBytes(bb.remaining())
                buf.writeBytes(bb)
        }
        val size = buf.writerIndex() - start
        if (size > maxSize)
            throw new MongoChannelException("Document is too large (" + size + " bytes but the max is " + maxSize + ")")
    }

    private[beaucatcher] def writeQuery[Q](buf: ChannelBuffer, query: Q, maxSize: Int)(implicit querySupport: QueryEncoder[Q]): Unit = {
        writeDocument(buf, query, maxSize)
    }

    private[beaucatcher] def writeEntity[E](buf: ChannelBuffer, entity: E, maxSize: Int)(implicit entitySupport: EntityEncodeSupport[E]): Unit = {
        writeDocument(buf, entity, maxSize)
    }

    private[beaucatcher] def readEntity[E](buf: ChannelBuffer)(implicit entitySupport: QueryResultDecoder[E]): E = {
        entitySupport match {
            case nettySupport: NettyDocumentDecoder[_] =>
                nettySupport.asInstanceOf[NettyDocumentDecoder[E]].read(buf)
            case _ =>
                entitySupport.decode(buf.toByteBuffer())
        }
    }
}
