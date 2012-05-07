package org.beaucatcher.mongo

import com.typesafe.config._

private[mongo] class ContextSettings(config: Config) {

    import scala.collection.JavaConverters._

    config.checkValid(ConfigFactory.defaultReference(), "beaucatcher")

    private val mongo = config.getConfig("beaucatcher.mongo")

    val driverClassName = mongo.getString("driver")

    val hosts: Seq[String] = if (mongo.hasPath("connection.hosts"))
        mongo.getStringList("connection.hosts").asScala
    else
        Seq.empty // FIXME from URI

    val defaultDatabaseName = if (mongo.hasPath("connection.default-database-name"))
        mongo.getString("connection.default-database-name")
    else
        "" // FIXME from URI

    private def uriFromParts: String = {
        // FIXME not quite there yet ;-)
        "mongodb://" + hosts.mkString(",") + "/" + defaultDatabaseName
    }

    // this will have to get  more complex to use the other stuff
    // in the config file
    val uri: String = if (mongo.hasPath("uri"))
        mongo.getString("uri")
    else
        uriFromParts
}
