package org.beaucatcher.mongo

import org.beaucatcher.bson._

trait WriteResult extends CommandResult {
    def n: Int

    def upserted: Option[Any]

    def updatedExisting: Option[Boolean]

    // unlike case class default toString this shows the field names
    override def toString =
        "WriteResult(ok=%s,errmsg=%s,err=%s,code=%s,n=%s,updatedExisting=%s,upserted=%s)".format(ok, errmsg, err, code, n, updatedExisting, upserted)

    override def toException() = new WriteResultMongoException(this)
}

private[mongo] case class WriteResultImpl(command: CommandResult, n: Int, upserted: Option[Any], updatedExisting: Option[Boolean]) extends WriteResult {

    override def ok: Boolean = command.ok
    override def errmsg: Option[String] = command.errmsg
    override def err: Option[String] = command.err
    override def code: Option[Int] = command.code
}

object WriteResult {
    def apply(raw: BObject): WriteResult = {
        WriteResultImpl(CommandResult(raw),
            n = CommandResult.getInt(raw, "n").getOrElse(0),
            updatedExisting = CommandResult.getBoolean(raw, "updatedExisting"),
            upserted = raw.get("upserted") map { _.unwrapped })
    }

    def apply(ok: Boolean, err: Option[String] = None, n: Int = 0,
        code: Option[Int] = None, upserted: Option[AnyRef] = None,
        updatedExisting: Option[Boolean] = None): WriteResult = {
        WriteResultImpl(CommandResultImpl(ok = ok, err = err, errmsg = None, code = code), n = n, upserted = upserted, updatedExisting = updatedExisting)
    }

    def apply(commandResult: CommandResult, n: Int,
        upserted: Option[AnyRef],
        updatedExisting: Option[Boolean]): WriteResult = {
        WriteResultImpl(command = commandResult, n = n, upserted = upserted, updatedExisting = updatedExisting)
    }
}

class WriteResultMongoException(result: WriteResult)
    extends CommandResultMongoException(result, "write failed: " + MongoExceptionUtils.commandResultToMessage(result)) {

}
