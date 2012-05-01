package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import akka.actor.ActorSystem
import java.net.InetSocketAddress
import java.net.URI

final class ChannelDriverContext private[cdriver] (override val driver: ChannelDriver, url: String, actorSystem: ActorSystem)
    extends DriverContext {

    // FIXME using java.net.URI will barf on some mongo URLs so
    // we need our own parser
    private val configURI = new URI(url)
    private val dbName = {
        val p = configURI.getPath()
        if (p.startsWith("/"))
            p.substring(1)
        else
            p
    }
    private val host = Option(configURI.getHost()).getOrElse("localhost")
    private val port = {
        val p = configURI.getPort()
        if (p < 0)
            27017
        else
            p
    }

    private lazy val driverConnection = new Connection(driver.backend, actorSystem, new InetSocketAddress(host, port))
    private[cdriver] def connection = driverConnection

    override type DriverType = ChannelDriver
    override type DatabaseType = ChannelDriverDatabase

    // since we aren't a wrapper, this is all just no-op
    override type UnderlyingConnectionType = Unit
    override type UnderlyingDatabaseType = Unit
    override type UnderlyingCollectionType = Unit

    private def noUnderlying() = throw new MongoException("No point trying to get underlying from ChannelDriver")

    override def underlyingConnection = noUnderlying()
    override def underlyingDatabase = noUnderlying()
    override def underlyingCollection(name: String) = noUnderlying()

    override final lazy val database = {
        new ChannelDriverDatabase(this, dbName)
    }

    override def close(): Unit = {
        connection.close()
    }
}
