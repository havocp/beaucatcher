package org.beaucatcher.mongo

trait CommandResult {
    def ok: Boolean
    def errmsg: Option[String]
    def err: Option[String]
    def code: Option[Int]

    // unlike case class default toString this shows the field names
    override def toString =
        "CommandResult(ok=%s,errmsg=%s,err=%s,code=%s)".format(ok, errmsg, err, code)

    /**
     * Convert a non-OK result into a thrown exception. If you have a method foo() returning
     * a CommandResult or WriteResult, use foo().throwIfNotOk to get an exception instead of
     * manually inspecting the result.
     */
    def throwIfNotOk(): this.type = {
        if (ok) {
            //System.err.println("not throwing: " + this)
            this
        } else {
            //System.err.println("throwing: " + this)
            // this is what the Java driver does... maybe to cope with various
            // mongo versions or something?
            if (code == Some(11000) || code == Some(11001) ||
                (err.isDefined && (err.get.startsWith("E11000") || err.get.startsWith("E11001"))))
                throw new DuplicateKeyMongoException(MongoExceptionUtils.commandResultToMessage(this))
            else
                throw toException()
        }
    }

    protected[beaucatcher] def toException() = new CommandResultMongoException(this)
}

private[mongo] case class CommandResultImpl(ok: Boolean,
    errmsg: Option[String], err: Option[String],
    code: Option[Int])
    extends CommandResult {

    // getLastError reply can violate this, we're supposed to
    // fix it before we get here.
    require(ok == (err.isEmpty && errmsg.isEmpty))
}

object CommandResult {
    private[mongo] def getBoolean(raw: Map[String, Any], key: String): Option[Boolean] = {
        raw.get(key) match {
            case Some(null) => None
            case Some(v: Boolean) => Some(v)
            case Some(v: Number) => Some(v == 1)
            case _ => None
        }
    }

    private[mongo] def getString(raw: Map[String, Any], key: String): Option[String] = {
        raw.get(key) match {
            case Some(null) => None
            case Some(v: String) => Some(v)
            case _ => None
        }
    }

    private[mongo] def getInt(raw: Map[String, Any], key: String): Option[Int] = {
        raw.get(key) match {
            case Some(null) => None
            case Some(v: Number) => Some(v.intValue)
            case _ => None
        }
    }

    def apply(raw: TraversableOnce[(String, Any)]): CommandResult = {
        val m = raw.toMap
        CommandResultImpl(
            ok = getBoolean(m, "ok").getOrElse(throw new BugInSomethingMongoException("Missing or invalid 'ok' field in command result: " + raw)),

            errmsg = getString(m, "errmsg"),

            err = getString(m, "err"),
            code = getInt(m, "code"))
    }

    def apply(ok: Boolean, errmsg: Option[String] = None, err: Option[String] = None, code: Option[Int] = None): CommandResult = {
        CommandResultImpl(ok = ok, errmsg = errmsg, err = err, code = code)
    }
}

class CommandResultMongoException(val result: CommandResult, message: String) extends MongoException(message) {
    def this(result: CommandResult) = this(result, "command failed: " + MongoExceptionUtils.commandResultToMessage(result))
}

private[mongo] object MongoExceptionUtils {
    private[mongo] def commandResultToMessage(result: CommandResult) = {
        (result.code map { _ + ": " }).getOrElse("") +
            result.errmsg.getOrElse(result.err.getOrElse("(no err or errmsg field)"))
    }
}
