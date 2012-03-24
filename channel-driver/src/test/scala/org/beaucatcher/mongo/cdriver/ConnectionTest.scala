package org.beaucatcher.mongo.cdriver

import org.beaucatcher.bson.Implicits._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.wire.{ mongo => wire }
import org.beaucatcher.channel._
import org.beaucatcher.channel.netty._
import org.beaucatcher.mongo.cdriver._
import org.junit.Assert._
import org.junit._
import java.nio.ByteOrder
import java.net.SocketAddress
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import java.util.Random
import akka.actor._
import akka.dispatch._
import akka.pattern._
import akka.util.duration._
import akka.util._

class ConnectionTest extends TestUtils {

    def block[T](f: Future[T]): T = {
        Await.result(f, 1 second)
    }

    def withFactory(body: (NettyMongoSocketFactory) => Unit): Unit = {
        val system = ActorSystem("ConnectionTest")
        val factory = new NettyMongoSocketFactory()(system.dispatcher)
        body(factory)
        // don't put these in a finally{} because exceptions from
        // them will then hide earlier exceptions
        block(factory.close())
        system.shutdown()
    }

    @Test
    def connectAndQuery(): Unit = withFactory { factory =>
        import Support._
        val futureSocket = factory.connect(new InetSocketAddress("localhost", wire.DEFAULT_PORT))
        val socket = block(futureSocket)

        val futureReply = socket.sendCommand(0, /* flags */
            "admin", BObject("ismaster" -> 1))

        val reply = block(futureReply)
        val doc = reply.iterator().next()
        assertEquals(true, doc.getUnwrappedAs[Boolean]("ismaster"))
    }

    private def assertDocumentSmallEnough(socket: MongoSocket, b: Binary): Unit = {
        import Support._

        val futureReply = socket.sendCommand(0, /* flags */
            "admin", BObject("ismaster" -> 1, "ignored" -> b))

        val reply = block(futureReply)
        val doc = reply.iterator().next()
        assertEquals(true, doc.getUnwrappedAs[Boolean]("ismaster"))
    }

    private def assertDocumentTooLarge(socket: MongoSocket, b: Binary): Unit = {
        import Support._

        val futureReply = socket.sendCommand(0, /* flags */
            "admin", BObject("ismaster" -> 1, "ignored" -> b))

        val e = intercept[MongoException] {
            block(futureReply)
        }
        assertTrue(e.getMessage.contains("too large"))
    }

    @Test
    def maxDocumentSize(): Unit = withFactory { factory =>
        val futureSocket = factory.connect(new InetSocketAddress("localhost", wire.DEFAULT_PORT))
        val socket = block(futureSocket)

        val oneByte = Binary(new Array[Byte](1))
        val oneK = Binary(new Array[Byte](1024))
        val tenK = Binary(new Array[Byte](1024 * 10))

        assertDocumentSmallEnough(socket, oneByte)
        assertDocumentSmallEnough(socket, oneK)
        assertDocumentSmallEnough(socket, tenK)

        socket.maxDocumentSize = 1
        assertDocumentTooLarge(socket, oneByte)
        assertDocumentTooLarge(socket, oneK)

        socket.maxDocumentSize = 1024 * 2
        assertDocumentSmallEnough(socket, oneByte)
        assertDocumentSmallEnough(socket, oneK)
        assertDocumentTooLarge(socket, tenK)
    }
}
