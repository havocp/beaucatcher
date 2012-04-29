package org.beaucatcher.mongo.cdriver

import Support._
import akka.actor._
import akka.dispatch._
import akka.pattern._
import akka.util._
import akka.util.duration._
import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.channel._
import org.beaucatcher.channel.netty._
import java.net.SocketAddress

private[cdriver] case class GetLastError(database: String, w: Int)

private[cdriver] object GetLastError {
    def safe(database: String): GetLastError = GetLastError(database, w = 1)
}

private[cdriver] case class DecodedResult(result: CommandResult, fields: Map[String, Any])
private[cdriver] case class DecodedWriteResult(result: WriteResult, fields: Map[String, Any])

private[cdriver] class SocketBatch(val reply: QueryReply, val cursor: SocketCursor) extends Batch[QueryReply] {

    override def isLastBatch = reply.cursorId == 0L
    override def executor = cursor.system.dispatcher
    override def iterator() = Iterator(reply)
    override def getMore() = {
        cursor.actor
            .map(_.ask(MongoCursorActor.GetBatch)(Connection.longTimeout)
                .map({
                    case MongoCursorActor.Batch(reply) =>
                        new SocketBatch(reply, cursor)
                }))
            .getOrElse(throw new MongoException("likely bug in library: actor should have existed if batches remained"))
    }
}

private[cdriver] class SocketCursor(val system: ActorSystem, val actor: Option[ActorRef],
    val firstReply: QueryReply) extends AsyncCursor[QueryReply] {

    override def close(): Unit = {
        actor.foreach(_ ! MongoCursorActor.Close)
    }

    override def firstBatch(): Batch[QueryReply] = {
        new SocketBatch(firstReply, this)
    }
}

/**
 * This wraps a group of MongoSocket, potentially handling: replica sets, socket pooling,
 * auto-reconnect, cursors, and getLastError.
 *
 * Note: does not actually handle all of this yet.
 */
private[cdriver] class Connection(private[cdriver] val system: ActorSystem, private val addr: SocketAddress) {

    import Connection._

    private val actor = system.actorOf(Props(new ConnectionActor(addr)))

    private def acquireSocket(): Future[MongoSocket] = {
        actor.ask(ConnectionActor.AcquireSocket)(longTimeout).map({
            case ConnectionActor.SocketAcquired(socket) =>
                socket
        })
    }

    def sendQuery[Q, F](flags: Int, fullCollectionName: String, numberToSkip: Int,
        numberToReturn: Int, limit: Long, query: Q, fieldsOption: Option[F])(implicit querySupport: QueryEncoder[Q], fieldsSupport: QueryEncoder[F]): Future[AsyncCursor[QueryReply]] = {
        acquireSocket()
            .flatMap({ socket =>
                socket.sendQuery(flags, fullCollectionName, numberToSkip, numberToReturn, query, fieldsOption)
                    .map({ reply => reply.throwOnError(); reply })
                    .map((socket, _))
            })
            .flatMap({
                case (socket, reply) =>
                    val remaining = limit - reply.numberReturned
                    if (reply.cursorId == 0L || remaining <= 0) {
                        Promise.successful(new SocketCursor(system, None, reply))(system.dispatcher)
                    } else {
                        actor.ask(ConnectionActor.CreateCursorActor(socket, fullCollectionName, numberToReturn, remaining, reply.cursorId))(longTimeout)
                            .map({
                                case ConnectionActor.CursorActorCreated(cursorActor) =>
                                    new SocketCursor(system, Some(cursorActor), reply)
                            })
                    }
            })
    }

    def sendQueryOne[Q, F](flags: Int, fullCollectionName: String, query: Q, fieldsOption: Option[F])(implicit querySupport: QueryEncoder[Q], fieldsSupport: QueryEncoder[F]): Future[Option[QueryReply]] = {
        acquireSocket()
            .flatMap({ socket =>
                socket.sendQuery(flags, fullCollectionName, 0 /* skip */ , -1 /* num to return, no cursor */ , query, fieldsOption)
                    .map({ reply =>
                        reply.throwOnError()
                        if (reply.numberReturned == 0)
                            None
                        else
                            Some(reply)
                    })
            })
    }

    def sendUpdate[Q, E](fullCollectionName: String, flags: Int,
        query: Q, update: E, gle: GetLastError)(implicit querySupport: QueryEncoder[Q], entitySupport: QueryEncoder[E]): Future[WriteResult] = {
        acquireSocket().flatMap(withLastError(_, gle, _.sendUpdate(fullCollectionName, flags, query, update)))
    }

    def sendInsert[E](flags: Int, fullCollectionName: String, docs: Traversable[E], gle: GetLastError)(implicit entitySupport: EntityEncodeSupport[E]): Future[WriteResult] = {
        acquireSocket().flatMap(withLastError(_, gle, _.sendInsert(flags, fullCollectionName, docs)))
    }

    def sendDelete[Q](fullCollectionName: String, flags: Int, query: Q, gle: GetLastError)(implicit querySupport: QueryEncoder[Q]): Future[WriteResult] = {
        acquireSocket().flatMap(withLastError(_, gle, _.sendDelete(fullCollectionName, flags, query)))
    }

    // note that we're going through hoops to be sure the GLE
    // goes down the same TCP socket as the request
    private def withLastError(socket: MongoSocket, gle: GetLastError, first: (MongoSocket) => Future[Unit]): Future[WriteResult] = {
        import RawEncoded._
        val step1 = first(socket)
        val raw = RawEncoded()
        raw.writeField("getlasterror", 1)
        raw.writeField("w", gle.w)
        raw.writeField("fsync", false)
        val step2 = socket.sendCommand(0 /* flags */ , gle.database, raw)
        step1.flatMap(_ => step2.map({ reply =>
            val result = decodeWriteResult(reply)
            result.result
        }))
    }

    /**
     * Close all the sockets and free resources.
     */
    def close(): Future[Unit] = {
        actor.ask(ConnectionActor.Close)(longTimeout).map({
            case ConnectionActor.Closed =>
                ()
        })
    }

    def sendCommand[Q](flags: Int, ns: String, query: Q)(implicit querySupport: QueryEncoder[Q]): Future[QueryReply] = {
        acquireSocket().flatMap(_.sendCommand(flags, ns, query))
    }
}

object Connection {
    // this is just "long" to let the underlying Netty or whatever timeout
    // win; we're hoping to get an error before this.
    private[cdriver] val longTimeout = 1 minutes
}

private[cdriver] class ConnectionActor(val addr: SocketAddress) extends Actor {

    import ConnectionActor._

    val socketFactory = new NettyMongoSocketFactory()(context.system.dispatcher)
    var socketCache: Option[MongoSocket] = None
    var pendingSocket: Option[Future[MongoSocket]] = None

    private def newSocket(): Future[MongoSocket] = {
        socketFactory.connect(addr) flatMap { socket =>
            socket.sendCommand(0 /* flags */ , "admin", BObject("ismaster" -> 1)) map { reply =>
                val doc = reply.iterator().next()
                if (doc.getUnwrappedAs[Boolean]("ismaster")) {
                    val maxSize = doc.getUnwrappedAs[Int]("maxBsonObjectSize")
                    socket.maxDocumentSize = maxSize
                    socket
                } else {
                    throw new MongoException("Connected to non-master node " + addr)
                }
            }
        }
    }

    override def receive = {
        case AcquireSocket =>
            if (socketCache.isDefined) {
                sender ! SocketAcquired(socketCache.get)
            } else {
                pendingSocket = Some(pendingSocket.getOrElse({
                    val sf = newSocket()
                    sf.map(SocketCreated(_)).pipeTo(self)
                    sf
                }))
                pendingSocket.get.map(SocketAcquired(_)).pipeTo(sender)
            }

        case SocketCreated(socket) =>
            socket.addCloseListener({ socket =>
                self ! SocketClosed(socket)
            })

            // always replace socketCache with the latest,
            // even if it's still set, maybe we created a new
            // socket because the old one had an error for example.
            socketCache = Some(socket)

        case SocketClosed(socket) =>
            if (socketCache == Some(socket)) {
                socketCache = None
            }

        case Close =>
            socketFactory.close()
            sender ! Closed

        // we create the cursor actor inside the ConnectionActor so that it isn't
        // a root actor; actors related to a connection are inside the ConnectionActor
        case CreateCursorActor(socket, fullCollectionName, batchSize, limit, cursorId) =>
            val actor =
                context.actorOf(Props(new MongoCursorActor(socket, fullCollectionName, batchSize, limit, cursorId)))
            sender ! CursorActorCreated(actor)
    }
}

object ConnectionActor {
    // Sent to the ConnectionActor
    case object AcquireSocket
    case object Close
    case class SocketCreated(socket: MongoSocket)
    case class SocketClosed(socket: MongoSocket)
    case class CreateCursorActor(socket: MongoSocket, fullCollectionName: String, batchSize: Int, limit: Long, cursorId: Long)

    // Sent from the ConnectionActor
    case class SocketAcquired(socket: MongoSocket)
    case object Closed
    case class CursorActorCreated(actor: ActorRef)
}
