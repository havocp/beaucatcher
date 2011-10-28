package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

class CommandResult(lazyRaw : => BObject) {
    lazy val raw = lazyRaw

    protected[beaucatcher] def getBoolean(key : String) : Option[Boolean] = {
        raw.get(key) match {
            case Some(BBoolean(v)) => Some(v)
            case Some(v : BNumericValue[_]) => Some(v == 1)
            case _ => None
        }
    }

    protected[beaucatcher] def getString(key : String) : Option[String] = {
        raw.get(key) match {
            case Some(BString(v)) => Some(v)
            case _ => None
        }
    }

    protected[beaucatcher] def getInt(key : String) : Option[Int] = {
        raw.get(key) match {
            case Some(v : BNumericValue[_]) => Some(v.intValue)
            case _ => None
        }
    }

    def ok : Boolean = {
        getBoolean("ok").getOrElse(throw new BugInSomethingMongoException("Missing or invalid 'ok' field in command result: " + raw))
    }

    def errmsg : Option[String] = {
        getString("errmsg")
    }

    def err : Option[String] = {
        getString("err")
    }

    def code : Option[Int] = {
        getInt("code")
    }

    override def toString = "CommandResult(ok=%s,errmsg=%s,err=%s,code=%s)".format(ok, errmsg, err, code)

    /**
     * Convert a non-OK result into a thrown exception. If you have a method foo() returning
     * a CommandResult or WriteResult, use foo().throwIfNotOk to get an exception instead of
     * manually inspecting the result.
     */
    def throwIfNotOk() {
        if (!ok)
            throw new CommandResultMongoException(this)
    }
}

object CommandResult {
    def apply(lazyRaw : => BObject) : CommandResult = new CommandResult(lazyRaw)

    def apply(ok : Boolean) : CommandResult = new CommandResult(BObject("ok" -> ok))
}
