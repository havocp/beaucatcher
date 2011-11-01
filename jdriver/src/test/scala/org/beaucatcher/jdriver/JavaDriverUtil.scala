package org.beaucatcher.jdriver

import org.beaucatcher.mongo.SyncDAO
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo._
import org.beaucatcher.jdriver._
import org.joda.time.DateTime

trait JavaDriverTestProvider
    extends MongoConfigProvider
    with JavaDriverBackendProvider {
    override val mongoConfig = new SimpleMongoConfig("beaucatcherjdriver", "localhost", 27017)
}

trait JavaDriverDatabaseTestProvider
    extends MongoConfigProvider
    with JavaDriverBackendProvider {
    override val mongoConfig = new SimpleMongoConfig("beaucatcherjdriverdb", "localhost", 27017)
}
