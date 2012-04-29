package org.beaucatcher.channel.netty

import akka.dispatch._
import org.beaucatcher.channel._
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.ByteOrder

object NettyChannelBackend extends ChannelBackend {

    override def newSocketFactory()(implicit executionContext: ExecutionContext) =
        new NettyMongoSocketFactory()

    override def newDynamicEncodeBuffer(preallocateSize: Int) =
        Buffer(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, preallocateSize))
}
