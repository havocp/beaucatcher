package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.wire._
import com.mongodb.{ CommandResult => _, _ }

private[casbah] class CasbahSyncDatabase(override protected val database : CasbahDatabase) extends SyncDatabase {
    override def command(cmd : BObject, options : CommandOptions) : CommandResult = {
        import j.JavaConversions._
        import Implicits._

        val casbahDB = database.backend.underlyingDatabase
        // the Java driver users 0 for default flags, rather than db.getOptions, not sure why. we copy it though.
        // I guess we probably want to deprecate the global options since they aren't a thread-safe approach.
        val flags : Int = options.overrideQueryFlags map { flags => flags : Int } getOrElse 0
        casbahDB.command(new BObjectDBObject(cmd), flags)
    }
}

private[casbah] class CasbahDatabase(override protected[casbah] val backend : CasbahBackend) extends Database {
    override lazy val sync = new CasbahSyncDatabase(this)
}
