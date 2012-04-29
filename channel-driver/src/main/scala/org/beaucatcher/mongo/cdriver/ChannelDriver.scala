package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.channel._
import akka.actor.ActorSystem

final class ChannelDriver private[cdriver] (private[cdriver] val backend: ChannelBackend)
    extends Driver {

    override final def createCollectionFactory[EntityType <: AnyRef: Manifest, IdType: Manifest](collectionName: String,
        caseClassBObjectQueryComposer: QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer: EntityComposer[EntityType, BObject]): CollectionFactory[EntityType, IdType, IdType] = {
        // TODO the query composer is just ignored by the channel driver.
        // The composer thing is pretty much BS in the new regime
        // anyway, we'll have to clean it up.
        new EntityBObjectChannelDriverCollectionFactory(this, collectionName,
            caseClassBObjectEntityComposer)
    }

    override def createCollectionFactoryWithoutEntity[IdType: Manifest](collectionName: String): CollectionFactoryWithoutEntity[IdType] = {
        new BObjectChannelDriverCollectionFactory(this, collectionName)
    }

    def newContext(config: MongoConfig, system: ActorSystem): Context = {
        new ChannelDriverContext(this, config, system)
    }
}

object ChannelDriver {

    // FIXME channel backend would come from a config file if we had multiple options
    // TODO we can break this build dependency using reflection; might be nice.
    lazy val instance = new ChannelDriver(org.beaucatcher.channel.netty.NettyChannelBackend)
}

/**
 * Mix this trait into a subclass of [[org.beaucatcher.mongo.CollectionAccess]] to backend
 * the collection operations using ChannelDriver
 */
trait ChannelDriverProvider extends DriverProvider {

    override def mongoDriver: ChannelDriver =
        ChannelDriver.instance
}
