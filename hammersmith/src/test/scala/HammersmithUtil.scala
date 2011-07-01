import org.beaucatcher.mongo.SyncDAO
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo._
import org.beaucatcher.hammersmith._

trait HammersmithTestProvider
    extends MongoConfigProvider
    with HammersmithBackendProvider {
    override val mongoConfig = new SimpleMongoConfig("beaucatcherhammersmith", "localhost", 27017)
}
