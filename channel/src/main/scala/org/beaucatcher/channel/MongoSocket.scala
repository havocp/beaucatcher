package org.beaucatcher.channel

import akka.dispatch._
import java.util.concurrent.atomic.AtomicInteger
import org.beaucatcher.wire._
import org.beaucatcher.mongo._

/**
 * An abstract low-level connection to a mongo server. This exposes the "raw" protocol
 * and an implementation basically just has to marshal to the wire and match up request/reply IDs.
 * The implementation of this should represent a single open socket.
 */
trait MongoSocket {
    private[this] val _maxDocumentSize = new AtomicInteger(Mongo.DEFAULT_MAX_DOCUMENT_SIZE)

    def maxDocumentSize = _maxDocumentSize.get

    def maxDocumentSize_=(value: Int): Unit = {
        _maxDocumentSize.set(value)
    }

    /**
     * Send an OP_QUERY. Parameters map directly to protocol and are in the same order as on the wire.
     * Weirdness note: negative numberToReturn means don't create a cursor and return the absolute value
     * number of max results. positive 1 also means don't create a cursor and return 1 result. 0 means
     * return a default number of results. All other positive numbers mean return initial number of
     * results and also a cursor ID.
     */
    def sendQuery[Q, F](flags: Int, fullCollectionName: String, numberToSkip: Int,
        numberToReturn: Int, query: Q, fieldsOption: Option[F])(implicit querySupport: QueryEncoder[Q],
            fieldsSupport: QueryEncoder[F]): Future[QueryReply]

    /** Send an OP_GETMORE. Parameters map directly to protocol and are in the same order as on the wire. */
    def sendGetMore(fullCollectionName: String, numberToReturn: Int, cursorId: Long): Future[QueryReply]

    /**
     * Send an OP_UPDATE. No reply.
     * Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendUpdate[Q, E](fullCollectionName: String, flags: Int,
        query: Q, update: E)(implicit querySupport: QueryEncoder[Q], entitySupport: QueryEncoder[E]): Future[Unit]

    /**
     * Send an OP_INSERT. No reply.
     * Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendInsert[E](flags: Int, fullCollectionName: String, docs: Traversable[E])(implicit entitySupport: EntityEncodeSupport[E]): Future[Unit]

    /**
     * Send an OP_DELETE. No reply.
     * Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendDelete[Q](fullCollectionName: String, flags: Int, query: Q)(implicit querySupport: QueryEncoder[Q]): Future[Unit]

    /**
     * Send an OP_KILL_CURSORS. No reply.
     */
    def sendKillCursors(cursorIds: Traversable[Long]): Future[Unit]

    /**
     * Close the socket and free resources.
     */
    def close(): Future[Unit]

    final def sendCommand[Q](flags: Int, ns: String, query: Q)(implicit querySupport: QueryEncoder[Q]): Future[QueryReply] = {
        sendQuery(flags, ns + ".$cmd", 0 /* skip */ , 1 /* return */ , query, None)
    }

    def addCloseListener(listener: (MongoSocket) => Unit): Unit
}

/** Reply to OP_QUERY */
trait QueryReply {
    def responseFlags: Int
    def cursorId: Long
    def startingFrom: Int
    def numberReturned: Int
    def iterator[E]()(implicit entitySupport: QueryResultDecoder[E]): Iterator[E]

    final def throwOnError(): Unit = {
        if ((responseFlags & Mongo.REPLY_FLAG_CURSOR_NOT_FOUND) != 0)
            throw new MongoException("Cursor ID was not valid anymore")
        // TODO we should parse $err here and include it in the message
        if ((responseFlags & Mongo.REPLY_FLAG_QUERY_FAILURE) != 0)
            throw new MongoException("Query failed")
    }
}
