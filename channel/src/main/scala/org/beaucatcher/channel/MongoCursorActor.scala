package org.beaucatcher.channel

import akka.actor._
import akka.dispatch._
import akka.util.duration._
import akka.pattern._
import akka.util._
import org.beaucatcher.mongo.MongoException

private[beaucatcher] object MongoCursorActor {
    // received by cursor actor
    case object Close
    case object GetBatch
    case object TimedOut
    case class GotMore(reply: QueryReply)

    // sent by cursor actor
    case class Batch(reply: QueryReply)
}

private[beaucatcher] class MongoCursorActor(val socket: MongoSocket, val fullCollectionName: String, val batchSize: Int, val limit: Long, val cursorId: Long) extends Actor {
    import MongoCursorActor._

    var killed = false
    var done = false
    var pending = false
    var lastActivity = System.currentTimeMillis
    var remaining = limit

    val cursorTimeout = 1 minute

    override def preStart = {
        context.system.scheduler.schedule(cursorTimeout, cursorTimeout / 10, self, TimedOut)
    }

    override def postStop = {
        if (!killed) {
            killed = true
            socket.sendKillCursors(List(cursorId))
        }
    }

    override def receive = {
        case GetBatch =>
            lastActivity = System.currentTimeMillis
            if (done) {
                throw new MongoException("No more items in cursor on " + fullCollectionName)
            } else if (pending) {
                throw new MongoException("Can only get one batch at a time from cursor on " + fullCollectionName)
            } else {
                pending = true
                // an important issue here is that errors also go back
                // to the sender, which is a reason we use pipeTo
                val f: Future[Batch] = socket.sendGetMore(fullCollectionName, batchSize, cursorId).flatMap({ reply =>
                    reply.throwOnError()
                    self.ask(GotMore(reply))(cursorTimeout).mapTo[Batch]
                })
                f.pipeTo(sender)
            }
        case GotMore(reply) =>
            lastActivity = System.currentTimeMillis
            if (reply.cursorId == 0) {
                done = true
                // no need to kill if we finish the cursor
                killed = true
            }
            pending = false

            remaining -= reply.numberReturned
            if (remaining <= 0) {
                done = true
                self ! PoisonPill
            }

            // the sender here is always ourselves
            sender ! Batch(reply)
        case Close =>
            self ! PoisonPill
        case TimedOut =>
            val elapsed = System.currentTimeMillis - lastActivity
            if (elapsed > cursorTimeout.toMillis) {
                self ! Close
            }
    }
}
