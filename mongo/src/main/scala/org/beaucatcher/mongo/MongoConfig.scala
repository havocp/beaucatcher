package org.beaucatcher.mongo

/**
 * A mixin trait that provides a [[org.beaucatcher.mongo.MongoConfig]] to the class
 * you mix it into.
 */
trait MongoConfigProvider {
    val mongoConfig: MongoConfig
}

/**
 * A configuration for connecting to a specific database, through one or more servers.
 * The configuration expected in most of Beaucatcher should not specify a collection, but must specify a database.
 */
trait MongoConfig {
    val url: String

    override def equals(other: Any): Boolean =
        other match {
            case that: MongoConfig =>
                (that canEqual this) &&
                    url == that.url
            case _ => false
        }

    def canEqual(other: Any): Boolean =
        other.isInstanceOf[MongoConfig]

    override def hashCode: Int =
        url.hashCode

    override def toString =
        "MongoConfig(%s)".format(url)
}

/** A simple configuration with just db, a single host, and port */
case class SimpleMongoConfig(val databaseName: String,
    val host: String,
    val port: Int) extends MongoConfig {
    override val url = "mongodb://%s:%d/%s".format(host, port, databaseName)
}

/**
 * The most general config can express anything found in the Mongo URL
 * http://www.mongodb.org/display/DOCS/Connections
 */
case class UrlMongoConfig(override val url: String)
    extends MongoConfig {
}
