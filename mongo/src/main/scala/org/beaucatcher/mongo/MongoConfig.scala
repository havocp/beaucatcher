package org.beaucatcher.mongo
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Lock

trait MongoConfigProvider {
    val mongoConfig : MongoConfig
}

trait MongoConfig {
    val databaseName : String
    val host : String
    val port : Int

    override def equals(other : Any) : Boolean =
        other match {
            case that : MongoConfig =>
                (that canEqual this) &&
                    databaseName == that.databaseName &&
                    host == that.host &&
                    port == that.port
            case _ => false
        }

    def canEqual(other : Any) : Boolean =
        other.isInstanceOf[MongoConfig]

    override def hashCode : Int =
        41 * (41 * (41 + databaseName.hashCode) + host.hashCode) + port

    override def toString =
        "MongoConfig(%s,%s,%s)".format(databaseName, host, port)
}

class SimpleMongoConfig(override val databaseName : String,
    override val host : String,
    override val port : Int) extends MongoConfig {

}

private[beaucatcher] case class MongoConnectionAddress(host : String, port : Int)

private[beaucatcher] abstract class MongoConnectionStore[ConnectionType] {
    private val map = new ConcurrentHashMap[MongoConnectionAddress, ConnectionType]
    private val creationLock = new Lock

    protected def create(address : MongoConnectionAddress) : ConnectionType

    def ensure(address : MongoConnectionAddress) : ConnectionType = {
        val c = map.get(address)
        if (c != null) {
            c
        } else {
            creationLock.acquire()
            val result = {
                val beatenToIt = map.get(address)
                if (beatenToIt != null) {
                    beatenToIt
                } else {
                    val created = create(address)
                    map.put(address, created)
                    created
                }
            }
            creationLock.release()
            result
        }
    }
}
