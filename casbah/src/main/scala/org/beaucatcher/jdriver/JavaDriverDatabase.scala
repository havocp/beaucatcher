package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.wire._
import com.mongodb.{ CommandResult => _, _ }

private[jdriver] class JavaDriverSyncDatabase(override protected val database : JavaDriverDatabase) extends SyncDatabase {
    override def command(cmd : BObject, options : CommandOptions) : CommandResult = {
        import Implicits._

        val jdriverDB = database.backend.underlyingDatabase
        // the Java driver users 0 for default flags, rather than db.getOptions, not sure why. we copy it though.
        // I guess we probably want to deprecate the global options since they aren't a thread-safe approach.
        val flags : Int = options.overrideQueryFlags map { flags => flags : Int } getOrElse 0
        jdriverDB.command(new BObjectDBObject(cmd), flags)
    }
}

private[jdriver] class JavaDriverDatabase(override protected[jdriver] val backend : JavaDriverBackend) extends Database {
    override lazy val sync = new JavaDriverSyncDatabase(this)

    override def name = backend.underlyingDatabase.getName()
}
