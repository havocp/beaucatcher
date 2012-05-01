package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import org.beaucatcher.channel._
import akka.actor.ActorSystem

final class ChannelDriver private[cdriver] (private[cdriver] val backend: ChannelBackend)
    extends Driver {

    private[beaucatcher] override def newBObjectCodecSet[IdType: IdEncoder](): CollectionCodecSet[BObject, BObject, IdType, BValue] =
        Codecs.newBObjectCodecSet()

    private[beaucatcher] override def newCaseClassCodecSet[EntityType <: Product: Manifest, IdType: IdEncoder](): CollectionCodecSet[BObject, EntityType, IdType, Any] =
        Codecs.newCaseClassCodecSet()

    private[beaucatcher] override def newStringIdEncoder(): IdEncoder[String] =
        Codecs.stringIdEncoder

    private[beaucatcher] override def newObjectIdIdEncoder(): IdEncoder[ObjectId] =
        Codecs.objectIdIdEncoder

    private[beaucatcher] override def newBObjectBasedCodecs[E](toBObject: (E) => BObject,
        fromBObject: (BObject) => E): BObjectBasedCodecs[E] = {
        import Codecs._
        BObjectBasedCodecs[E](toBObject, fromBObject)
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
