package org.beaucatcher.wire

object Mongo {
    // struct { int messageLength, int requestId, int responseTo, int opCode }
    val MESSAGE_HEADER_LENGTH = 4 * 4

    val DEFAULT_PORT = 27017

    val DEFAULT_MAX_DOCUMENT_SIZE = 1024 * 1024 * 4

    val OP_REPLY = 1 // Reply to a client request. responseTo is set.
    val OP_MSG = 1000 // generic msg command followed by a string; no reply
    val OP_UPDATE = 2001 // update document; no reply
    val OP_INSERT = 2002 // insert new document; no reply
    // 2003 formerly used for OP_GET_BY_OID
    val OP_QUERY = 2004 // query a collection; has a reply
    val OP_GETMORE = 2005 // Get more data from a query; has a reply
    val OP_DELETE = 2006 // Delete documents; no reply
    val OP_KILL_CURSORS = 2007 // Tell database client is done with a cursor; no reply

    val QUERY_FLAG_TAILABLE_CURSOR = 1 << 1 // Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor", the cursor may become invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
    val QUERY_FLAG_SLAVE_OK = 1 << 2 // Allow query of replica slave. Normally these return an error except for namespace "local".
    val QUERY_FLAG_OPLOG_RELAY = 1 << 3 // Internal replication use only - driver should not set
    val QUERY_FLAG_NO_CURSOR_TIMEOUT = 1 << 4 // The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
    val QUERY_FLAG_AWAIT_DATA = 1 << 5 // Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
    val QUERY_FLAG_EXHAUST = 1 << 6 // Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
    val QUERY_FLAG_PARTIAL = 1 << 7 // Get partial results from a mongos if some shards are down (instead of throwing an error)

    val REPLY_FLAG_CURSOR_NOT_FOUND = 1 << 0 //Set when getMore is called but the cursor id is not valid at the server. Returned with zero results.
    val REPLY_FLAG_QUERY_FAILURE = 1 << 1 // Set when query failed. Results consist of one document containing an "$err" field describing the failure.
    val REPLY_FLAG_SHARD_CONFIG_STALE = 1 << 2 // Drivers should ignore this. Only mongos will ever see this set, in which case, it needs to update config from the server.
    val REPLY_FLAG_AWAIT_CAPABLE = 1 << 3 // Set when the server supports the AwaitData Query option. If it doesn't, a client should sleep a little between getMore's of a Tailable cursor. Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.

    val UPDATE_FLAG_UPSERT = 1 << 0 // insert the supplied object into the collection if no matching document is found.
    val UPDATE_FLAG_MULTI_UPDATE = 1 << 1 // update all matching objects rather than only first

    val INSERT_FLAG_CONTINUE_ON_ERROR = 1 << 0 // continue inserting other docs if one of them fails

    val DELETE_FLAG_SINGLE_REMOVE = 1 << 0 // remove only the first matching document instead of all
}
