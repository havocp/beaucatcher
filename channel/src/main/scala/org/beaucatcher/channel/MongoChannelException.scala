package org.beaucatcher.channel

import org.beaucatcher.mongo.MongoException

/** Exception arising from the network channel or protocol connecting us to mongod. */
class MongoChannelException(message: String, cause: Throwable) extends MongoException(message, cause) {
    def this(message: String) = this(message, null)
}
