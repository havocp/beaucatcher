package org.beaucatcher.mongo

class MongoException(message : String, cause : Throwable) extends Exception(message, cause) {
    def this(message : String) = this(message, null)
}

class DuplicateKeyMongoException(message : String, cause : Throwable) extends MongoException(message, cause) {
    def this(message : String) = this(message, null)
}

/**
 * Exception that indicates a bug in something (mongod itself or the library).
 */
class BugInSomethingMongoException(message : String, cause : Throwable) extends MongoException(message, cause) {
    def this(message : String) = this(message, null)
}
