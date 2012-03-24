package org.beaucatcher.channel

import org.beaucatcher.mongo.MongoException

class MongoChannelException(message: String, cause: Throwable) extends MongoException(message, cause) {
    def this(message: String) = this(message, null)
}
