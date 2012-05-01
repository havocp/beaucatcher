package org.beaucatcher.mongo

import java.nio.charset.Charset
import org.beaucatcher.wire._

// All these private utility methods would need cleaning up if made public
private[beaucatcher] object CodecUtils {

    private[this] val zeroByte = 0.toByte
    private[this] val utf8Charset = Charset.forName("UTF-8");

    def writeNulString(buf: EncodeBuffer, s: String): Unit = {
        val bytes = s.getBytes(utf8Charset)
        buf.ensureWritableBytes(bytes.length + 1)
        buf.writeBytes(bytes)
        buf.writeByte('\0')
    }

    def writeLengthString(buf: EncodeBuffer, s: String): Unit = {
        val bytes = s.getBytes(utf8Charset)
        buf.ensureWritableBytes(bytes.length + 1)
        buf.writeInt(bytes.length + 1) // + 1 for nul
        buf.writeBytes(bytes)
        buf.writeByte('\0')
    }

    def readNulString(buf: DecodeBuffer): String = {
        val start = buf.readerIndex
        val len = buf.bytesBefore(zeroByte)
        val bytes = new Array[Byte](len)
        buf.readBytes(bytes)
        buf.readByte() // mop up the nul
        new String(bytes, utf8Charset)
    }

    def readLengthString(buf: DecodeBuffer): String = {
        val len = buf.readInt()
        val bytes = new Array[Byte](len - 1) // -1 for nul
        buf.readBytes(bytes)
        buf.readByte() // mop up the nul
        new String(bytes, utf8Charset)
    }

    def skipNulString(buf: DecodeBuffer): Unit = {
        val len = buf.bytesBefore(zeroByte)
        buf.skipBytes(len + 1)
    }

    def skipLengthString(buf: DecodeBuffer): Unit = {
        val len = buf.readInt()
        buf.skipBytes(len)
    }

    def skipDocument(buf: DecodeBuffer): Unit = {
        val len = buf.readInt()
        buf.skipBytes(len - 4)
    }

    def writeEmptyQuery(buf: EncodeBuffer): Unit = {
        buf.ensureWritableBytes(5)
        buf.writeInt(Bson.EMPTY_DOCUMENT_LENGTH)
        buf.writeByte(zeroByte)
    }

    def writeDocument[D](buf: EncodeBuffer, doc: D, maxSize: Int)(implicit encodeSupport: DocumentEncoder[D]): Unit = {
        val start = buf.writerIndex
        encodeSupport.encode(buf, doc)
        val size = buf.writerIndex - start
        if (size > maxSize)
            throw new DocumentTooLargeMongoException("Document is too large (" + size + " bytes but the max is " + maxSize + ")")
    }

    def writeQuery[Q](buf: EncodeBuffer, query: Q, maxSize: Int)(implicit querySupport: QueryEncoder[Q]): Unit = {
        writeDocument(buf, query, maxSize)
    }

    def writeUpdateQuery[Q](buf: EncodeBuffer, query: Q, maxSize: Int)(implicit querySupport: UpdateQueryEncoder[Q]): Unit = {
        writeDocument(buf, query, maxSize)
    }

    def writeModifier[E](buf: EncodeBuffer, entity: E, maxSize: Int)(implicit entitySupport: ModifierEncoder[E]): Unit = {
        writeDocument(buf, entity, maxSize)
    }

    def writeUpsert[E](buf: EncodeBuffer, entity: E, maxSize: Int)(implicit entitySupport: UpsertEncoder[E]): Unit = {
        writeDocument(buf, entity, maxSize)
    }

    def writeIdField[I](buf: EncodeBuffer, name: String, id: I)(implicit idEncoder: IdEncoder[I]): Unit = {
        idEncoder.encodeField(buf, name, id)
    }

    def readEntity[E](buf: DecodeBuffer)(implicit entitySupport: QueryResultDecoder[E]): E = {
        entitySupport.decode(buf)
    }

    def readValue[V](what: Byte, buf: DecodeBuffer)(implicit valueDecoder: ValueDecoder[V]): V = {
        valueDecoder.decode(what, buf)
    }
}
