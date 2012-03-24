package org.beaucatcher.channel

import akka.dispatch._
import java.util.concurrent.atomic.AtomicInteger
import org.beaucatcher.wire.mongo._

/**
 * An abstract low-level connection to a mongo server. This exposes the "raw" protocol
 * and an implementation basically just has to marshal to the wire and match up request/reply IDs.
 * The implementation of this should represent a single open socket.
 */
trait MongoSocket {
    private[this] val _maxDocumentSize = new AtomicInteger(DEFAULT_MAX_DOCUMENT_SIZE)

    def maxDocumentSize = _maxDocumentSize.get

    def maxDocumentSize_=(value: Int): Unit = {
        _maxDocumentSize.set(value)
    }

    /**
     * Send an OP_QUERY. Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendQuery[Q](flags: Int, fullCollectionName: String, numberToSkip: Int,
        numberToReturn: Int, query: Q, fieldsOption: Option[Q])(implicit querySupport: QueryEncodeSupport[Q]): Future[QueryReply]

    /** Send an OP_GETMORE. Parameters map directly to protocol and are in the same order as on the wire. */
    def sendGetMore(fullCollectionName: String, numberToReturn: Int, cursorId: Long): Future[QueryReply]

    /**
     * Send an OP_UPDATE. No reply.
     * Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendUpdate[Q, E](fullCollectionName: String, flags: Int,
        query: Q, update: E)(implicit querySupport: QueryEncodeSupport[Q], entitySupport: EntityEncodeSupport[E]): Future[Unit]

    /**
     * Send an OP_INSERT. No reply.
     * Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendInsert[E](flags: Int, fullCollectionName: String, docs: Traversable[E])(implicit entitySupport: EntityEncodeSupport[E]): Future[Unit]

    /**
     * Send an OP_DELETE. No reply.
     * Parameters map directly to protocol and are in the same order as on the wire.
     */
    def sendDelete[Q](fullCollectionName: String, flags: Int, query: Q)(implicit querySupport: QueryEncodeSupport[Q]): Future[Unit]

    /**
     * Send an OP_KILL_CURSORS. No reply.
     */
    def sendKillCursors(cursorIds: Traversable[Long]): Future[Unit]

    /**
     * Close the socket and free resources.
     */
    def close(): Future[Unit]

    final def sendCommand[Q](flags: Int, ns: String, query: Q)(implicit querySupport: QueryEncodeSupport[Q]): Future[QueryReply] = {
        sendQuery(flags, ns + ".$cmd", 0 /* skip */ , 1 /* return */ , query, None)
    }
}

/** Iterate over entities decoded from BSON documents */
trait EntityIterator {
    def hasNext: Boolean
    def next[E]()(implicit entitySupport: EntityDecodeSupport[E]): E
}

/** Reply to OP_QUERY */
trait QueryReply {
    def responseFlags: Int
    def cursorId: Long
    def startingFrom: Int
    def numberReturned: Int
    def iterator(): EntityIterator
}
