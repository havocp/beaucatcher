/**
 * Copyright (c) 2012 Havoc Pennington
 * Some code in this file from Hammersmith,
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.beaucatcher.channel.netty

import org.beaucatcher.mongo._
import org.beaucatcher.channel._
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.net.SocketAddress
import java.nio.ByteOrder
import akka.dispatch._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.DirectChannelBufferFactory
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.jboss.netty.channel.SimpleChannelHandler
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictor
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.ReceiveBufferSizePredictor
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.group.ChannelGroupFutureListener
import org.jboss.netty.channel.group.ChannelGroupFuture
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.InternalLogLevel
import org.jboss.netty.logging.InternalLoggerFactory
import org.jboss.netty.logging.AbstractInternalLogger
import org.jboss.netty.channel.ExceptionEvent

class NettyMongoSocketFactory(implicit private val executor: ExecutionContext) {

    NettyMongoSocketFactory.debugLoggingEnabled // want this to happen before we use netty

    // this limit is only intended to keep memory usage
    // from going nuts. Possibly it should be configurable.
    private val maxIncomingFrameSize = 1024 * 1024 * 128

    // this only ever changes in one direction, false to true
    @volatile private var closed = false

    private final class NamedThreadFactory(val name: String)
        extends ThreadFactory {

        private val nextSerial = new AtomicInteger(1)

        override def newThread(r: Runnable): Thread = {
            val t = new Thread(r)
            t.setName("%s %d".format(name, nextSerial.incrementAndGet))
            t.setDaemon(true)
            t
        }
    }

    private final class MongoFrameDecoder extends LengthFieldBasedFrameDecoder(
        maxIncomingFrameSize, /* max frame length */
        0, /* length field offset: length at start of the message */
        4, /* length field length: 4-byte integer */
        -4, /* length adjustment: tell netty length field was part of the length */
        0, /* bytes to strip; we just leave the length field there */
        true) /* fail fast: whether to actually read the whole frame before throwing TooLongFrameException */ {

        // we could override extractFrame to avoid a copy if we were going to synchronously convert
        // the buffer in decode(), but we aren't, so we copy here to avoid work later.

        // we could override decode() to log incoming frames or otherwise customize.
    }

    // this sees both upstream (incoming) and downstream (outgoing) traffic
    private final class MongoChannelHandler(val mongoSocketPromise: Promise[NettyMongoSocket]) extends SimpleChannelHandler {
        var socketOption: Option[NettyMongoSocket] = None
        override def messageReceived(context: ChannelHandlerContext, e: MessageEvent): Unit = {
            for (socket <- socketOption) {
                e.getMessage match {
                    case buf: ChannelBuffer =>
                        socket.receivedFrame(context.getChannel(), buf)
                }
            }
        }

        override def channelConnected(context: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            val socket = new NettyMongoSocket(context.getChannel())

            socketOption = Some(socket)

            socket.connected(context.getChannel())

            mongoSocketPromise.success(socket)
        }

        override def channelDisconnected(context: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            for (socket <- socketOption) {
                socket.disconnected()
            }
        }

        override def channelClosed(context: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
            if (socketOption.isEmpty) {
                // we were never connected; if we were connected, then channelDisconnected
                // will handle things for us.
                mongoSocketPromise.failure(new MongoChannelException("Channel closed"))
            }
        }

        override def exceptionCaught(context: ChannelHandlerContext, e: ExceptionEvent): Unit = {
            val f = e.getFuture()
            val cause = e.getCause() match {
                case m: MongoException => m
                case x => new MongoChannelException(x.getMessage(), x)
            }

            f.setFailure(cause)
        }
    }

    private lazy val channelFactory =
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(new NamedThreadFactory("Beaucatcher Netty Boss")),
            Executors.newCachedThreadPool(new NamedThreadFactory("Beaucatcher Netty Worker")),
            1, /* max boss count */
            2 * Runtime.getRuntime().availableProcessors()) /* max worker count */

    private lazy val allChannels = new DefaultChannelGroup("mongo sockets")

    def close(): Future[Unit] = synchronized {
        closed = true

        val p = Promise[Unit]()
        allChannels.close().addListener(new ChannelGroupFutureListener() {
            override def operationComplete(group: ChannelGroupFuture): Unit = {
                // we have to kick this async again or else releaseExternalResources()
                // will deadlock since we're in the close() callback.
                executor.execute(new Runnable() {
                    override def run(): Unit = {
                        try {
                            channelFactory.releaseExternalResources()
                        } finally {
                            p.success()
                        }
                    }
                })
            }
        })
        p
    }

    def connect(addr: SocketAddress): Future[NettyMongoSocket] = {
        val mongoSocketPromise = Promise[NettyMongoSocket]
        val decoder = new MongoFrameDecoder
        val handler = new MongoChannelHandler(mongoSocketPromise)
        val pipeline = {
            if (NettyMongoSocketFactory.debugLoggingEnabled) {
                val logging = new LoggingHandler("mongo pipeline", InternalLogLevel.DEBUG, true /* hexDump */ )
                Channels.pipeline(decoder, handler, logging)
            } else {
                Channels.pipeline(decoder, handler)
            }
        }

        val channel = channelFactory.newChannel(pipeline)

        val open = synchronized {
            if (closed) {
                // this should complete the mongoSocketPromise
                // by closing the channel
                channel.close()
                false
            } else {
                allChannels.add(channel)
                true
            }
        }

        if (open) {
            val channelConfig = channel.getConfig()
            channelConfig.setKeepAlive(true)
            channelConfig.setTcpNoDelay(true)

            // because our MongoFrameDecoder will copy the frames, we
            // go ahead and use a direct buffer for the brief time until
            // the copy. But this needs profiling probably.
            // also here we're setting LITTLE_ENDIAN (mongo is always little endian)
            channelConfig.setBufferFactory(DirectChannelBufferFactory.getInstance(ByteOrder.LITTLE_ENDIAN))

            channelConfig match {
                case nioConfig: NioSocketChannelConfig =>
                    // this predictor is based on how many bytes read() returned
                    // in the last few read() calls.
                    val predictor = new AdaptiveReceiveBufferSizePredictor(128, // min
                        512, // initial
                        1024 * 1024 * 4) // max, 4M per read()
                    nioConfig.setReceiveBufferSizePredictor(predictor)
            }

            channel.connect(addr)
        }

        mongoSocketPromise
    }
}

private[beaucatcher] object NettyMongoSocketFactory {
    // remove "false &&" to get debug-spammed
    lazy val debugLoggingEnabled = false && {
        val logger = new AbstractInternalLogger() {
            private def log(s: String): Unit = System.err.println(s)
            override def debug(s: String) = log(s)
            override def debug(s: String, t: Throwable) = log(s)
            override def isDebugEnabled() = true
            override def info(s: String) = log(s)
            override def info(s: String, t: Throwable) = log(s)
            override def isInfoEnabled() = true
            override def warn(s: String) = log("WARN: " + s)
            override def warn(s: String, t: Throwable) = log("WARN: " + s)
            override def isWarnEnabled() = true
            override def error(s: String) = log("ERROR: " + s)
            override def error(s: String, t: Throwable) = log("ERROR: " + s)
            override def isErrorEnabled() = true
        }
        InternalLoggerFactory.setDefaultFactory(new InternalLoggerFactory() {
            override def newInstance(s: String) = logger
        });
        System.err.println("Initialized netty debug noise")
        true
    }
}
