package org.beaucatcher.driver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.dispatch.Future

private[beaucatcher] trait SyncDriverDatabase {
    protected def database: DriverDatabase

    private implicit def context = database.context

    // TODO take any query not BObject
    def command(cmd: BObject, options: CommandOptions): CommandResult
}

private[beaucatcher] trait AsyncDriverDatabase {
    protected def database: DriverDatabase

    private implicit def context = database.context

    // TODO take any query not BObject
    def command(cmd: BObject, options: CommandOptions): Future[CommandResult]
}

private[beaucatcher] trait DriverDatabase {
    protected def driver: Driver

    def context: DriverContext

    def name: String

    def sync: SyncDriverDatabase

    def async: AsyncDriverDatabase
}
