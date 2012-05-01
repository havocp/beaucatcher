package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import com.mongodb.{ CommandResult => _, _ }
import akka.dispatch.Future

private[jdriver] class JavaDriverSyncDatabase(override protected val database : JavaDriverDatabase) extends SyncDriverDatabase {
    override def command(cmd : BObject, options : CommandOptions) : CommandResult =
        database.command(cmd, options)
}

private[jdriver] class JavaDriverAsyncDatabase(override protected val database : JavaDriverDatabase) extends AsyncDriverDatabase {
    override def command(cmd : BObject, options : CommandOptions) : Future[CommandResult] =
        Future({ database.command(cmd, options) } : CommandResult)(database.context.actorSystem.dispatcher)
}

private[jdriver] class JavaDriverDatabase(override val context : JavaDriverContext)
    extends DriverDatabase {
    override def driver = context.driver

    override lazy val sync = new JavaDriverSyncDatabase(this)

    override lazy val async = new JavaDriverAsyncDatabase(this)

    override def name = context.underlyingDatabase.getName()

    private[jdriver] def command(cmd : BObject, options : CommandOptions) : CommandResult = {
        import Implicits._
        import JavaConversions._

        val jdriverDB = context.underlyingDatabase
        val flags : Int = JavaDriverDatabase.commandFlags(options)
        jdriverDB.command(new BObjectDBObject(cmd), flags)
    }
}

private[jdriver] object JavaDriverDatabase {
    private[jdriver] def commandFlags(options : CommandOptions) : Int = {
        // the Java driver users 0 for default flags, rather than db.getOptions, not sure why. we copy it though.
        // I guess we probably want to deprecate the global options since they aren't a thread-safe approach.
        options.overrideQueryFlags map { flags => flags : Int } getOrElse 0
    }
}
