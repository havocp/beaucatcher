package org.beaucatcher.hammersmith

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb.async._
import com.mongodb.async.util._
import org.bson.types.{ ObjectId => JavaObjectId }

private[hammersmith] class HammersmithBackend(private val config : MongoConfig)
    extends MongoBackend {

    val hammersmithURI = new MongoURI(config.url)

    if (!hammersmithURI.db.isDefined)
        throw new IllegalArgumentException("Must specify the database name in mongodb URI")

    /**
     * Hammersmith's connection object used by this backend.
     */
    lazy val connection = HammersmithBackend.connections.ensure(MongoConnectionAddress(config.url))

    private def collection(name : String) : Collection = {
        if (name == null)
            throw new IllegalArgumentException("null collection name")

        val db : DB = connection(hammersmithURI.db.get)
        assert(db != null)
        val coll : Collection = db(name)
        assert(coll != null)
        coll
    }

    override final def createDAOGroup[EntityType <: Product : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType] = {
        val identityIdComposer = new IdentityIdComposer[IdType]
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            new CaseClassBObjectHammersmithDAOGroup[EntityType, IdType, IdType, JavaObjectId](collection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                HammersmithBackend.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            val hammersmithIdComposer = new IdComposer[IdType, AnyRef] {
                override def idOut(id : AnyRef) : IdType = id.asInstanceOf[IdType]
                override def idIn(id : IdType) : AnyRef = id.asInstanceOf[AnyRef]
            }
            new CaseClassBObjectHammersmithDAOGroup[EntityType, IdType, IdType, AnyRef](collection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                hammersmithIdComposer)
        }
    }
}

private[hammersmith] object HammersmithBackend {
    lazy val scalaToJavaObjectIdComposer = new IdComposer[ObjectId, JavaObjectId] {
        import j.JavaConversions._
        override def idIn(id : ObjectId) : JavaObjectId = id
        override def idOut(id : JavaObjectId) : ObjectId = id
    }

    val connections = new MongoConnectionStore[MongoConnection] {
        override def create(address : MongoConnectionAddress) = {
            val (c, _, _) = MongoConnection.fromURI(address.url)
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
        new HammersmithBackend(mongoConfig)
    }
}
