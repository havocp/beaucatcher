package org.beaucatcher.mongo

class MongoException(message: String, cause: Throwable) extends Exception(message, cause) {
    def this(message: String) = this(message, null)
}

class MongoDocumentTooLargeException(message: String, cause: Throwable) extends MongoException(message, cause) {
    def this(message: String) = this(message, null)
}
