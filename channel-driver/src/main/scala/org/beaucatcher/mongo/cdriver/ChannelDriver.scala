package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import org.beaucatcher.channel._
import akka.actor.ActorSystem
import com.typesafe.config.Config

final class ChannelDriver private[mongo] (val config: Config, val loader: ClassLoader)
    extends Driver {

    private val backendName = config.getString("beaucatcher.mongo.channel-driver.backend")

    private[cdriver] val backend: ChannelBackend = {
        val klass = try {
            loader.loadClass(backendName)
        } catch {
            case e: ClassNotFoundException =>
                throw new MongoException("Configured channel driver backend '" + backendName + "' not found", e)
        }
        try {
            klass.getDeclaredConstructor(classOf[Config]).newInstance(config).asInstanceOf[ChannelBackend]
        } catch {
            case e: Exception =>
                throw new MongoException("Failed to instantiate '" + backendName + "': " + e.getMessage, e)
        }
    }

    private[beaucatcher] override def newSyncCollection(name: String)(implicit context: DriverContext): SyncDriverCollection = {
        SyncDriverCollection.fromAsync(newAsyncCollection(name))
    }

    private[beaucatcher] override def newAsyncCollection(name: String)(implicit context: DriverContext): AsyncDriverCollection = {
        new ChannelDriverAsyncCollection(name, context.asChannelContext)
    }

    private[beaucatcher] override def newContext(url: String, system: ActorSystem): DriverContext = {
        new ChannelDriverContext(this, url, system)
    }
}
