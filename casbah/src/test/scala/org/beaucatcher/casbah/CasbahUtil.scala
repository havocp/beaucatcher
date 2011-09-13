package org.beaucatcher.casbah

import org.beaucatcher.mongo.SyncDAO
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo._
import org.beaucatcher.casbah._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._
import org.joda.time.DateTime

trait CasbahTestProvider
    extends MongoConfigProvider
    with CasbahBackendProvider {
    override val mongoConfig = new SimpleMongoConfig("beaucatchercasbah", "localhost", 27017)
}

trait CasbahDatabaseTestProvider
    extends MongoConfigProvider
    with CasbahBackendProvider {
    override val mongoConfig = new SimpleMongoConfig("beaucatchercasbahdb", "localhost", 27017)
}
