package org.beaucatcher.mongo

trait MongoConfigProvider {
    val mongoConfig : MongoConfig
}

trait MongoConfig {
    val databaseName : String
    val host : String
    val port : Int
}

class SimpleMongoConfig(override val databaseName : String,
    override val host : String,
    override val port : Int) extends MongoConfig {

}
