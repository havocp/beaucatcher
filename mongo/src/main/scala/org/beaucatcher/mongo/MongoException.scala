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

class CommandResultMongoException(val result : CommandResult, message : String) extends MongoException(message) {
    def this(result : CommandResult) = this(result, "command failed: " + MongoException.commandResultToMessage(result))
}

class WriteResultMongoException(result : WriteResult) extends CommandResultMongoException(result, "write failed: " + MongoException.commandResultToMessage(result)) {

}

private[mongo] object MongoException {
    private[mongo] def commandResultToMessage(result : CommandResult) = {
        (result.code map { _ + ": " }).getOrElse("") +
            result.errmsg.getOrElse(result.err.getOrElse("(no err or errmsg field)"))
    }
}
