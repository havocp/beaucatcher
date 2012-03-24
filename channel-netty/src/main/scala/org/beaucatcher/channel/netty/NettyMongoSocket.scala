package org.beaucatcher.channel.netty

import org.beaucatcher.channel._
import org.beaucatcher.wire.mongo._
import org.beaucatcher.wire.bson._
import akka.dispatch._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteOrder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelFuture

final class NettyMongoSocket(private val channel: Channel)(implicit private val executor: ExecutionContext) extends MongoSocket {

    private val nextSerial = new AtomicInteger(0)
    private val pending = new ConcurrentHashMap[Int, Promise[_]](4, /* initial capacity - we don't expect tons outstanding */
        0.75f, /* load factor - this is the default */
        1) /* concurrency level - this is 1 writer, vs. the default of 16 */

    private def newPending[T]()(implicit executor: ExecutionContext): (Int, Promise[T]) = {
        val i = nextSerial.getAndIncrement()
        val p = Promise[T]()
        val old = pending.put(i, p)
        if (old ne null)
            throw new RuntimeException("Somehow re-used the same message id?")
        (i, p)
    }

    private def removePending[T](serial: Int): Promise[T] = {
        val p = pending.remove(serial)
        if (p eq null)
            throw new RuntimeException("Got a reply with unknown ID " + serial)
        // put back the type parameter
        p.asInstanceOf[Promise[T]]
    }

    private def failPending(serial: Int, e: MongoChannelException): Unit = {
        val p = removePending(serial)
        p.failure(e)
    }

    private class NettyEntityIterator(val buf: ChannelBuffer) extends EntityIterator {
        def hasNext: Boolean =
            buf.readableBytes() > 0

        def next[E]()(implicit entitySupport: EntityDecodeSupport[E]): E = {
            if (hasNext)
                readEntity(buf)
            else
                throw new NoSuchElementException("No more documents in this iterator")
        }
    }

    private class NettyQueryReply(val buf: ChannelBuffer) extends QueryReply {
        // struct { int messageLength, int requestId, int responseTo, int opCode }
        // struct { int responseFlags, int64 cursorId, int startingFrom, int numReturned, document* documents }
        override lazy val responseFlags: Int = buf.getInt(MESSAGE_HEADER_LENGTH)
        override lazy val cursorId: Long = buf.getLong(MESSAGE_HEADER_LENGTH + 4)
        override lazy val startingFrom: Int = buf.getInt(MESSAGE_HEADER_LENGTH + 12)
        override lazy val numberReturned: Int = buf.getInt(MESSAGE_HEADER_LENGTH + 16)

        def iterator(): EntityIterator = {
            val offset = MESSAGE_HEADER_LENGTH + 20
            val documentsStart = buf.readerIndex + offset
            // slice() gives the iterator its own reader/writer indexes but
            // the data is still shared.
            new NettyEntityIterator(buf.slice(documentsStart, buf.readableBytes() - offset))
        }
    }

    private[this] def withQueryReply(body: (Int, Promise[NettyQueryReply]) => ChannelBuffer): Future[QueryReply] = {
        val (serial, promise) = newPending[NettyQueryReply]
        try {
            val buf = body(serial, promise)

            channel.write(buf)
        } catch {
            // if any synchronous exceptions occur, stuff them in the future and
            // drop the pending message. This way people don't have to both
            // handle exceptions on the future and handle exceptions synchronously.
            // (One likely exception here is "document too large")
            case e: Exception =>
                removePending(serial).failure(e)
        }

        promise
    }

    override def sendQuery[Q](flags: Int, fullCollectionName: String, numberToSkip: Int,
        numberToReturn: Int, query: Q,
        fieldsOption: Option[Q])(implicit querySupport: QueryEncodeSupport[Q]): Future[QueryReply] = {
        withQueryReply { (serial, promise) =>
            // struct { int messageLength, int requestId, int responseTo, int opCode }
            // struct { int flags, cstring fullCollectionName, int numberToSkip, int numberToReturn, doc query, [doc fields] }

            // note that fullCollectionName.length is in chars not utf-8 bytes. we're just estimating.
            val guessedLength = MESSAGE_HEADER_LENGTH + 4 + fullCollectionName.length + 4 + 4 + 128

            val buf = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, guessedLength)

            buf.writeInt(0) // length, to be fixed up later
            buf.writeInt(serial)
            buf.writeInt(0) // responseTo
            buf.writeInt(OP_QUERY)
            buf.writeInt(flags)
            writeNulString(buf, fullCollectionName)
            buf.writeInt(numberToSkip)
            buf.writeInt(numberToReturn)

            writeQuery(buf, query, maxDocumentSize)

            // mongod seems to allow either writing an empty
            // object for fields are omitting the object entirely.
            // some other drivers write it but we'll omit it as long
            // as that appears to work...
            for (fields <- fieldsOption)
                writeQuery(buf, fields, maxDocumentSize)

            val length = buf.writerIndex()
            buf.setInt(0, length)
            buf
        }
    }

    def sendGetMore(fullCollectionName: String, numberToReturn: Int, cursorId: Long): Future[QueryReply] = {
        withQueryReply { (serial, promise) =>
            // note that fullCollectionName.length is in chars not utf-8 bytes. we're just estimating.
            val guessedLength = MESSAGE_HEADER_LENGTH + 4 + fullCollectionName.length + 4 + 8

            val buf = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, guessedLength)

            buf.writeInt(0) // length, to be fixed up later
            buf.writeInt(serial)
            buf.writeInt(0) // responseTo
            buf.writeInt(OP_GETMORE)

            buf.writeInt(0) // reserved, must be zero
            writeNulString(buf, fullCollectionName)
            buf.writeInt(numberToReturn)
            buf.writeLong(cursorId)

            val length = buf.writerIndex()
            buf.setInt(0, length)
            buf
        }
    }

    override def close(): Future[Unit] = {
        val p = Promise[Unit]()
        channel.close().addListener(new ChannelFutureListener() {
            override def operationComplete(f: ChannelFuture): Unit = {
                p.success()
            }
        })
        p
    }

    private[netty] def receivedFrame(writeChannel: Channel, buf: ChannelBuffer): Unit = {
        require(writeChannel eq channel) // true for now afaik

        // struct { int messageLength, int requestId, int responseTo, int opCode }
        val request = buf.getInt(4)
        val responseTo = buf.getInt(8)
        val opCode = buf.getInt(12)

        if (opCode == OP_REPLY) {
            val p = removePending(responseTo).asInstanceOf[Promise[NettyQueryReply]]

            // note that the reply lazy-decodes, so decoding will happen when
            // someone actually tries to use the reply. We are relying on success()
            // running its callbacks async, which it appears to do. We don't
            // really want to do the decode work in this IO thread.
            p.success(new NettyQueryReply(buf))
        }
    }

    private[netty] def connected(writeChannel: Channel): Unit = {
        require(writeChannel eq channel) // true for now afaik
    }

    private[netty] def disconnected(): Unit = {
        while (!pending.isEmpty()) {
            val entryOption = try {
                Some(pending.entrySet().iterator().next())
            } catch {
                case e: NoSuchElementException =>
                    None
            }
            for (entry <- entryOption) {
                val serial = entry.getKey
                // modifies hash table breaking the iteration
                failPending(serial, new MongoChannelException("Disconnected"))
            }
        }
    }
}
