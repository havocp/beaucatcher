package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.channel._
import org.beaucatcher.driver._
import akka.dispatch._
import akka.util.duration._

private[cdriver] final class ChannelDriverSyncDatabase(override protected val database: ChannelDriverDatabase) extends SyncDriverDatabase {
    override def command(cmd: BObject, options: CommandOptions): CommandResult = {
        // FIXME don't hardcode the timeout
        Await.result(database.command(cmd, options), 1 minute)
    }
}

private[cdriver] final class ChannelDriverAsyncDatabase(override protected val database: ChannelDriverDatabase) extends AsyncDriverDatabase {
    override def command(cmd: BObject, options: CommandOptions): Future[CommandResult] = {
        database.command(cmd, options)
    }
}

private[cdriver] final class ChannelDriverDatabase(override val context: ChannelDriverContext, override val name: String)
    extends DriverDatabase {
    override def driver = context.driver

    override lazy val sync = new ChannelDriverSyncDatabase(this)

    override lazy val async = new ChannelDriverAsyncDatabase(this)

    private[cdriver] def command(cmd: BObject, options: CommandOptions): Future[CommandResult] = {
        import BObjectCodecs._

        context.connection.sendCommand(queryFlags(options.overrideQueryFlags), name, cmd)
            .map({ reply =>
                val result = decodeCommandResult(reply)
                result.result
            })
    }
}
