package org.beaucatcher.mongo

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
