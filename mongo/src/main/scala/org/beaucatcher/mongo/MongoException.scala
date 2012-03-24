package org.beaucatcher.mongo

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
    def this(result : CommandResult) = this(result, "command failed: " + MongoExceptionUtils.commandResultToMessage(result))
}

class WriteResultMongoException(result : WriteResult) extends CommandResultMongoException(result, "write failed: " + MongoExceptionUtils.commandResultToMessage(result)) {

}

private[mongo] object MongoExceptionUtils {
    private[mongo] def commandResultToMessage(result : CommandResult) = {
        (result.code map { _ + ": " }).getOrElse("") +
            result.errmsg.getOrElse(result.err.getOrElse("(no err or errmsg field)"))
    }
}
