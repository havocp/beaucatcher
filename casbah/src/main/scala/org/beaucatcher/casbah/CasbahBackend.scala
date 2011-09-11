package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb.casbah.MongoURI
import com.mongodb.casbah.Imports.{ ObjectId => JavaObjectId, _ }
import com.mongodb.casbah.commons.conversions.scala._
import org.joda.time.DateTime

/**
 * [[org.beaucatcher.casbah.CasbahBackend]] is final with a private constructor - there's no way to create one
 * directly. However, a [[org.beaucatcher.casbah.CasbahBackendProvider]] exposes a [[org.beaucatcher.casbah.CasbahBackend]] and
 * you may want to use `provider.backend.underlyingConnection`, `underlyingDB`, or `underlyingCollection`
 * to get at Casbah methods directly.
 */
final class CasbahBackend private[casbah] (private val config : MongoConfig)
    extends MongoBackend {

    private lazy val casbahURI = MongoURI(config.url)
    private lazy val connection = CasbahBackend.connections.ensure(MongoConnectionAddress(config.url))

    private def collection(name : String) : MongoCollection = {
        if (name == null)
            throw new IllegalArgumentException("null collection name")
        val db : MongoDB = connection(casbahURI.database)
        assert(db != null)
        val coll : MongoCollection = db(name)
        assert(coll != null)
        coll
    }

    override type ConnectionType = MongoConnection
    override type DatabaseType = MongoDB
    override type CollectionType = MongoCollection

    override def underlyingConnection : MongoConnection = connection
    override def underlyingDatabase(name : String) : MongoDB = connection(name)
    override def underlyingCollection(name : String) : MongoCollection = collection(name)

    override final def createDAOGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType] = {
        val identityIdComposer = new IdentityIdComposer[IdType]
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            // special-case ObjectId to map Beaucatcher ObjectId to the org.bson version
            new EntityBObjectCasbahDAOGroup[EntityType, IdType, IdType, JavaObjectId](collection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                CasbahBackend.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            new EntityBObjectCasbahDAOGroup[EntityType, IdType, IdType, IdType](collection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                identityIdComposer)
        }
    }
}

private[casbah] object CasbahBackend {

    lazy val scalaToJavaObjectIdComposer = new IdComposer[ObjectId, JavaObjectId] {
        import j.JavaConversions._
        override def idIn(id : ObjectId) : JavaObjectId = id
        override def idOut(id : JavaObjectId) : ObjectId = id
    }

    val connections = new MongoConnectionStore[MongoConnection] {
        override def create(address : MongoConnectionAddress) = {
            RegisterJodaTimeConversionHelpers()

            val c = MongoConnection(MongoURI(address.url))
            // things are awfully race-prone without Safe, and you
            // don't get constraint violations for example
            c.setWriteConcern(WriteConcern.Safe)
            c
        }
    }

}

/**
 * Mix this trait into a subclass of [[org.beaucatcher.mongo.CollectionOperations]] to backend
 * the collection operations using Casbah
 */
trait CasbahBackendProvider extends MongoBackendProvider {
    self : MongoConfigProvider =>

    override lazy val backend : CasbahBackend = {
        require(mongoConfig != null)
        new CasbahBackend(mongoConfig)
    }
}
