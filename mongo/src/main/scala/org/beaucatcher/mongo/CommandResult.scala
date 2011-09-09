package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

class CommandResult(val raw : BObject) {
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
}

object CommandResult {
    def apply(raw : BObject) : CommandResult = new CommandResult(raw)

    def apply(ok : Boolean) : CommandResult = new CommandResult(BObject("ok" -> ok))
}
