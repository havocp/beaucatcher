package org.beaucatcher.hammersmith

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb.async._

private[hammersmith] class HammersmithBackend(private val databaseName : String,
    private val host : String,
    private val port : Int) extends MongoBackend {

    /**
     * Hammersmith's connection object used by this backend.
     */
    lazy val connection = HammersmithBackend.connections.ensure(MongoConnectionAddress(host, port))

    private def collection(name : String) : Collection = {
        if (name == null)
            throw new IllegalArgumentException("null collection name")
        val db : DB = connection(databaseName)
        assert(db != null)
        val coll : Collection = db(name)
        assert(coll != null)
        coll
    }

    override final def createDAOGroup[EntityType <: Product : Manifest, IdType](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType] = {
        new CaseClassBObjectHammersmithDAOGroup[EntityType, IdType, IdType](collection(collectionName),
            caseClassBObjectQueryComposer,
            caseClassBObjectEntityComposer,
            new IdentityIdComposer[IdType])
    }
}

private[hammersmith] object HammersmithBackend {
    val connections = new MongoConnectionStore[MongoConnection] {
        override def create(address : MongoConnectionAddress) = {
            val c = MongoConnection(address.host, address.port)
            // things are awfully race-prone without Safe, and you
            // don't get constraint violations for example
            c.writeConcern = WriteConcern.Safe
            c
        }
    }
}

/**
 * Mix this trait into a subclass of [[org.beaucatcher.mongo.CollectionOperations]] to backend
 * the collection operations using Hammersmith
 */
trait HammersmithBackendProvider extends MongoBackendProvider {
    self : MongoConfigProvider =>

    override lazy val backend : MongoBackend = {
        require(mongoConfig != null)
        new HammersmithBackend(mongoConfig.databaseName, mongoConfig.host, mongoConfig.port)
    }
}
