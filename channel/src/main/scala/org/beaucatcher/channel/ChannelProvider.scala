package org.beaucatcher.channel
import akka.dispatch._
import java.net.SocketAddress
import java.io.Closeable
import org.beaucatcher.mongo._

trait SocketFactory {
    def connect(addr: SocketAddress): Future[MongoSocket]
    def close(): Future[Unit]
}

trait ChannelBackend {
    def newDynamicEncodeBuffer(preallocateSize: Int): EncodeBuffer
    def newSocketFactory()(implicit executionContext: ExecutionContext): SocketFactory
}
