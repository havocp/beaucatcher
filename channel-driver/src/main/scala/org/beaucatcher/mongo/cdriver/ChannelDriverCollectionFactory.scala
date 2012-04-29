package org.beaucatcher.mongo.cdriver

import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import akka.actor.ActorSystem
import org.jboss.netty.buffer.ChannelBuffer

private[cdriver] class BObjectChannelDriverCollectionFactory[IdType](
    val driver: ChannelDriver,
    val collectionName: String)
    extends CollectionFactoryWithoutEntity[IdType] {
    require(driver != null)
    require(collectionName != null)

    override def newBObjectSync(implicit outerContext: Context): BObjectSyncCollection[IdType] = {
        SyncCollection.fromAsync(newBObjectAsync)
    }

    override def newBObjectAsync(implicit outerContext: Context): BObjectAsyncCollection[IdType] = {
        new BObjectChannelDriverAsyncCollection[IdType] {
            override val context = outerContext.asChannelContext
            override val name = collectionName
        }
    }
}

// TODO all this "composer" BS is left over from the Java driver backend
// and can be mopped up sometime
private[cdriver] class EntityBObjectChannelDriverCollectionFactory[EntityType <: AnyRef: Manifest, EntityIdType, BObjectIdType](
    override val driver: ChannelDriver,
    override val collectionName: String,
    val entityBObjectEntityComposer: EntityComposer[EntityType, BObject])
    extends BObjectChannelDriverCollectionFactory[BObjectIdType](driver, collectionName)
    with CollectionFactory[EntityType, EntityIdType, BObjectIdType] {
    require(driver != null)
    require(collectionName != null)
    require(entityBObjectEntityComposer != null)

    implicit private object EntityEncodeSupport
        extends EntityEncodeSupport[EntityType] {
        override final def encode(buf: EncodeBuffer, t: EntityType): Unit = {
            val bobj = entityBObjectEntityComposer.entityIn(t)
            Codecs.BObjectEncodeSupport.encode(buf, bobj)
        }
    }

    implicit private object EntityDecodeSupport
        extends QueryResultDecoder[EntityType] {
        override final def decode(buf: DecodeBuffer): EntityType = {
            val bobj = Codecs.BObjectDecodeSupport.decode(buf)
            entityBObjectEntityComposer.entityOut(bobj)
        }
    }

    override def newEntitySync(implicit outerContext: Context): EntitySyncCollection[BObject, EntityType, EntityIdType] = {
        SyncCollection.fromAsync(newEntityAsync)
    }

    override def newEntityAsync(implicit outerContext: Context): EntityAsyncCollection[BObject, EntityType, EntityIdType] = {
        new EntityChannelDriverAsyncCollection[EntityType, EntityIdType](entityBObjectEntityComposer) {
            override val context = outerContext.asChannelContext
            override val name = collectionName
        }
    }
}
