package org.beaucatcher.jdriver

import com.mongodb._
import org.beaucatcher.mongo._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

private[jdriver] class ConnectionDestroyedException extends Exception("Connection was destroyed before it was acquired")

// FIXME this needs to be the set of ServerAddress, but not the database name
private[jdriver] case class ConnectionInfo(uri : MongoURI)

private[jdriver] class JavaDriverConnection(val info : ConnectionInfo, val underlying : Mongo) {
    private var refcount = 1
    private var destroyed = false

    def acquire() : Unit = synchronized {
        if (destroyed) {
            throw new ConnectionDestroyedException
        } else {
            refcount += 1
        }
    }

    def release() = synchronized {
        if (refcount < 1)
            throw new BugInSomethingMongoException("Released connection more times than it was acquired")
        refcount -= 1
        destroyed = (refcount == 0)
        destroyed
    }
}

private[jdriver] object JavaDriverConnection {

    private[jdriver] val active = new ConcurrentHashMap[ConnectionInfo, JavaDriverConnection](
        2, /* initial capacity - default is 16 */
        0.75f, /* load factor - this is the default */
        1) /* concurrency level - this is 1 writer, vs. the default of 16 */
    private val creationLock = new ReentrantLock

    private[jdriver] def acquireConnection(info : ConnectionInfo) : JavaDriverConnection = {
        val c = active.get(info)
        try {
            if (c eq null) {
                creationLock.lock()
                try {
                    val beatenToIt = active.get(info)
                    if (beatenToIt eq null) {
                        val underlying = new Mongo(info.uri)
                        // things are awfully race-prone without Safe, and you
                        // don't get constraint violations for example.
                        // Also since we do async with threads (with AsyncCollection)
                        // there is no real reason for people to use fire-and-forget
                        // with the Java driver itself, right?
                        underlying.setWriteConcern(WriteConcern.SAFE)
                        val created = new JavaDriverConnection(info, underlying)
                        active.put(info, created)
                        created
                    } else {
                        beatenToIt.acquire()
                        beatenToIt
                    }
                } finally {
                    creationLock.unlock()
                }
            } else {
                c.acquire()
                c
            }
        } catch {
            // if the connection is destroyed before we acquire it, start over
            case e : ConnectionDestroyedException =>
                acquireConnection(info)
        }
    }

    private[jdriver] def releaseConnection(connection : JavaDriverConnection) : Unit = {
        if (connection.release()) {
            creationLock.lock()
            try {
                if (active.get(connection.info) eq connection)
                    active.remove(connection.info)
            } finally {
                creationLock.unlock()
            }
            connection.underlying.close()
        }
    }
}
