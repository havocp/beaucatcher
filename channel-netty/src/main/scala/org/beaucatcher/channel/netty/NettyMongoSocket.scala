package org.beaucatcher.channel.netty

import org.beaucatcher.channel._
import org.beaucatcher.wire._
import org.beaucatcher.mongo._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern._
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

    private def newPending[T <: AnyRef]()(implicit executor: ExecutionContext): (Int, Promise[T]) = {
        val i = nextSerial.getAndIncrement()
        val p = Promise[T]()
        val old = pending.put(i, p)
        if (old ne null)
            throw new RuntimeException("Somehow re-used the same message id?")
        // on either error or success, remove from the map
        p.onComplete({ whatever => pending.remove(i) })
        (i, p)
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
        override lazy val responseFlags: Int = buf.getInt(Mongo.MESSAGE_HEADER_LENGTH)
        override lazy val cursorId: Long = buf.getLong(Mongo.MESSAGE_HEADER_LENGTH + 4)
        override lazy val startingFrom: Int = buf.getInt(Mongo.MESSAGE_HEADER_LENGTH + 12)
        override lazy val numberReturned: Int = buf.getInt(Mongo.MESSAGE_HEADER_LENGTH + 16)

        def iterator(): EntityIterator = {
            val offset = Mongo.MESSAGE_HEADER_LENGTH + 20
            val documentsStart = buf.readerIndex + offset
            // slice() gives the iterator its own reader/writer indexes but
            // the data is still shared.
            new NettyEntityIterator(buf.slice(documentsStart, buf.readableBytes() - offset))
        }
    }

    private[this] final def withQueryReply(body: (Int, Promise[NettyQueryReply]) => ChannelBuffer): Future[QueryReply] = {
        val (serial, promise) = newPending[NettyQueryReply]
        try {
            val buf = body(serial, promise)

            sendMessage(buf, promise)
        } catch {
            // if any synchronous exceptions occur, stuff them in the future and
            // drop the pending message. This way people don't have to both
            // handle exceptions on the future and handle exceptions synchronously.
            // (One likely exception here is "document too large")
            case e: Exception =>
                promise.failure(e)
        }

        promise
    }

    private[this] final def messageBuffer(serial: Int, op: Int, guessedAfterHeaderLength: Int): ChannelBuffer = {
        val buf = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN,
            Mongo.MESSAGE_HEADER_LENGTH + guessedAfterHeaderLength)
        buf.writeInt(0) // length, to be fixed up later
        buf.writeInt(serial)
        buf.writeInt(0) // responseTo, always 0 when sending from client
        buf.writeInt(op)
        buf
    }

    private class WriteErrorListener(val promise: Promise[_]) extends ChannelFutureListener {
        override final def operationComplete(f: ChannelFuture): Unit = {
            if (!f.isSuccess()) {
                promise.failure(new MongoChannelException("Writing message failed", f.getCause()))
            }
        }
    }

    private[this] final def sendMessage(buf: ChannelBuffer, promise: Promise[_]): Unit = {
        val length = buf.writerIndex()
        buf.setInt(0, length)
        channel.write(buf).addListener(new WriteErrorListener(promise))
    }

    override def sendQuery[Q](flags: Int, fullCollectionName: String, numberToSkip: Int,
        numberToReturn: Int, query: Q,
        fieldsOption: Option[Q])(implicit querySupport: QueryEncodeSupport[Q]): Future[QueryReply] = {
        withQueryReply { (serial, promise) =>
            // struct { int messageLength, int requestId, int responseTo, int opCode }
            // struct { int flags, cstring fullCollectionName, int numberToSkip, int numberToReturn, doc query, [doc fields] }

            // note that fullCollectionName.length is in chars not utf-8 bytes. we're just estimating.
            val buf = messageBuffer(serial, Mongo.OP_QUERY, 4 + fullCollectionName.length + 4 + 4 + 128)

            buf.writeInt(flags)
            writeNulString(buf, fullCollectionName)
            buf.ensureWritableBytes(8) // in case nul string was longer than guessed
            buf.writeInt(numberToSkip)
            buf.writeInt(numberToReturn)

            writeQuery(buf, query, maxDocumentSize)

            // mongod seems to allow either writing an empty
            // object for fields are omitting the object entirely.
            // some other drivers write it but we'll omit it as long
            // as that appears to work...
            for (fields <- fieldsOption)
                writeQuery(buf, fields, maxDocumentSize)

            buf
        }
    }

    def sendGetMore(fullCollectionName: String, numberToReturn: Int, cursorId: Long): Future[QueryReply] = {
        withQueryReply { (serial, promise) =>
            val buf = messageBuffer(serial, Mongo.OP_GETMORE,
                4 + fullCollectionName.length + 4 + 8)

            buf.writeInt(0) // reserved, must be zero
            writeNulString(buf, fullCollectionName)
            buf.ensureWritableBytes(12) // in case nul string was longer than guessed
            buf.writeInt(numberToReturn)
            buf.writeLong(cursorId)

            buf
        }
    }

    def sendUpdate[Q, E](fullCollectionName: String, flags: Int,
        query: Q, update: E)(implicit querySupport: QueryEncodeSupport[Q], entitySupport: EntityEncodeSupport[E]): Future[Unit] = {
        val promise = Promise[Unit]()

        val buf = messageBuffer(nextSerial.getAndIncrement(), Mongo.OP_UPDATE,
            4 + fullCollectionName.length + 4 + 128)

        buf.writeInt(0) // reserved, must be zero
        writeNulString(buf, fullCollectionName)
        buf.ensureWritableBytes(4) // in case nul string was longer than guessed
        buf.writeInt(flags)
        writeQuery(buf, query, maxDocumentSize)
        writeEntity(buf, update, maxDocumentSize)

        sendMessage(buf, promise)

        promise
    }

    def sendInsert[E](flags: Int, fullCollectionName: String,
        docs: Traversable[E])(implicit entitySupport: EntityEncodeSupport[E]): Future[Unit] = {
        val promise = Promise[Unit]()

        val buf = messageBuffer(nextSerial.getAndIncrement(), Mongo.OP_INSERT,
            4 + fullCollectionName.length + 128)

        buf.writeInt(flags)
        writeNulString(buf, fullCollectionName)
        for (doc <- docs)
            writeEntity(buf, doc, maxDocumentSize)

        sendMessage(buf, promise)

        promise
    }

    def sendDelete[Q](fullCollectionName: String, flags: Int,
        query: Q)(implicit querySupport: QueryEncodeSupport[Q]): Future[Unit] = {
        val promise = Promise[Unit]()

        val buf = messageBuffer(nextSerial.getAndIncrement(), Mongo.OP_DELETE,
            4 + fullCollectionName.length + 4 + 128)

        buf.writeInt(0) // reserved, zero
        writeNulString(buf, fullCollectionName)
        buf.ensureWritableBytes(4)
        buf.writeInt(flags)
        writeQuery(buf, query, maxDocumentSize)

        sendMessage(buf, promise)

        promise
    }

    def sendKillCursors(cursorIds: Traversable[Long]): Future[Unit] = {
        val promise = Promise[Unit]()

        val buf = messageBuffer(nextSerial.getAndIncrement(), Mongo.OP_KILL_CURSORS,
            4 + 4 + 16)

        buf.writeInt(0) // reserved, 0
        val lengthIndex = buf.writerIndex()
        buf.writeInt(0) // number of cursor IDs, will overwrite later
        var count = 0
        for (c <- cursorIds) {
            count += 1
            buf.ensureWritableBytes(8)
            buf.writeLong(c)
        }
        buf.setInt(lengthIndex, count)

        sendMessage(buf, promise)

        promise
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

    override def addCloseListener(listener: (MongoSocket) => Unit): Unit = {
        val socket = this
        channel.getCloseFuture().addListener(new ChannelFutureListener() {
            override def operationComplete(f: ChannelFuture): Unit = {
                listener(socket)
            }
        })
    }

    private[netty] def receivedFrame(writeChannel: Channel, buf: ChannelBuffer): Unit = {
        require(writeChannel eq channel) // true for now afaik

        // struct { int messageLength, int requestId, int responseTo, int opCode }
        val request = buf.getInt(4)
        val responseTo = buf.getInt(8)
        val opCode = buf.getInt(12)

        if (opCode == Mongo.OP_REPLY) {
            val p = pending.get(responseTo)

            if (p ne null) {
                val np = p.asInstanceOf[Promise[NettyQueryReply]]
                // note that the reply lazy-decodes, so decoding will happen when
                // someone actually tries to use the reply. We are relying on success()
                // running its callbacks async, which it appears to do. We don't
                // really want to do the decode work in this IO thread.
                np.success(new NettyQueryReply(buf))
            }
        }
    }

    private[netty] def connected(writeChannel: Channel): Unit = {
        require(writeChannel eq channel) // true for now afaik
    }

    private[netty] def disconnected(): Unit = {
        import scala.collection.JavaConverters._

        while (!pending.isEmpty()) {
            // pending.values changes as pending does; here,
            // force an immutable Scala List
            val promises = pending.values.asScala.toList

            for (p <- promises) {
                // this will kick off an asynchronous removal
                // of "p" from the "pending" map
                p.failure(new MongoChannelException("Disconnected"))
            }

            // a little hack to match the Future.reduce signature which requires AnyRef
            val futures: Traversable[Future[AnyRef]] = promises.map(_.asInstanceOf[Promise[AnyRef]])
            val all = Future.reduce(futures)({ (a, b) => ().asInstanceOf[AnyRef] })

            // wait for everything to be asynchronously canceled
            Await.result(all, 1 second)
        }
    }
}
