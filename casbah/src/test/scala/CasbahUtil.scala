import org.beaucatcher.mongo.SyncDAO
import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._
import org.beaucatcher.mongo.CaseClassSyncDAO
import org.beaucatcher.mongo.BObjectSyncDAO
import org.beaucatcher.casbah._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._
import org.joda.time.DateTime

object CasbahUtil {
    private lazy val dbname = "mongoscalathingy"
    private lazy val connection_ = {
        val host = "localhost"
        val port = 27017

        RegisterJodaTimeConversionHelpers()

        val c = MongoConnection(host, port)
        // things are awfully race-prone without Safe, and you
        // don't get constraint violations for example
        c.setWriteConcern(WriteConcern.Safe)
        Some(c)
    }

    lazy val connection = connection_.getOrElse(null)

    def collection(name : String) : MongoCollection = {
        if (name == null)
            throw new IllegalArgumentException("null collection name")
        val db : MongoDB = connection(dbname)
        assert(db != null)
        val coll : MongoCollection = db(name)
        assert(coll != null)
        coll
    }
}
