package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

class WriteResult(lazyRaw : => BObject) extends CommandResult(lazyRaw) {
    def n : Int = getInt("n").getOrElse(0)

    def upserted : Option[Any] = raw.get("upserted") map { _.unwrapped }

    def updatedExisting : Option[Boolean] = getBoolean("updatedExisting")

    override def toString =
        "WriteResult(ok=%s,errmsg=%s,err=%s,code=%s,n=%s,updatedExisting=%s,upserted=%s)".format(ok, errmsg, err, code, n, updatedExisting, upserted)

    override def throwIfNotOk() {
        if (!ok)
            throw new WriteResultMongoException(this)
    }
}

object WriteResult {
    def apply(lazyRaw : => BObject) : WriteResult = new WriteResult(lazyRaw)

    /** This constructor is basically for converting from a Hammersmith WriteResult */
    def apply(ok : Boolean, err : Option[String] = None, n : Int = 0,
        code : Option[Int] = None, upserted : Option[AnyRef] = None,
        updatedExisting : Option[Boolean] = None) : WriteResult = {
        val builder = BObject.newBuilder
        builder += ("ok" -> ok)
        builder += ("n" -> n)
        err foreach { e =>
            builder += ("err" -> e)
        }
        code foreach { i =>
            builder += ("code" -> i)
        }
        upserted foreach { u =>
            builder += ("upserted" -> BValue.wrap(u))
        }
        updatedExisting foreach { b =>
            builder += ("updatedExisting" -> b)
        }

        new WriteResult(builder.result)
    }
}
