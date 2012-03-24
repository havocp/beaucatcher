package org.beaucatcher.mongo

class MongoException(message: String, cause: Throwable) extends Exception(message, cause) {
    def this(message: String) = this(message, null)
}
