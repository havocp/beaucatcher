import com.ometer.mongo.SyncDAO
import com.ometer.bson._
import com.ometer.bson.Implicits._
import com.ometer.mongo.CaseClassSyncDAO
import com.ometer.mongo.BObjectSyncDAO
import com.ometer.casbah._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._
import org.joda.time.DateTime

object MongoUtil {
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
