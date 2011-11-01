package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb._
import org.bson.types.{ ObjectId => JavaObjectId, _ }
import org.joda.time.DateTime

/**
 * [[org.beaucatcher.casbah.CasbahBackend]] is final with a private constructor - there's no way to create one
 * directly. However, a [[org.beaucatcher.casbah.CasbahBackendProvider]] exposes a [[org.beaucatcher.casbah.CasbahBackend]] and
 * you may want to use `provider.backend.underlyingConnection`, `underlyingDB`, or `underlyingCollection`
 * to get at Casbah methods directly.
 */
final class CasbahBackend private[casbah] (override val config : MongoConfig)
    extends MongoBackend {

    private lazy val casbahURI = new MongoURI(config.url)
    private lazy val connection = CasbahBackend.connections.ensure(MongoConnectionAddress(config.url))

    override type ConnectionType = Mongo
    override type DatabaseType = DB
    override type CollectionType = DBCollection

    override def underlyingConnection : Mongo = connection
    override def underlyingDatabase : DB = connection.getDB(casbahURI.getDatabase())
    override def underlyingCollection(name : String) : DBCollection = {
        if (name == null)
            throw new IllegalArgumentException("null collection name")
        val db : DB = underlyingDatabase
        assert(db != null)
        val coll : DBCollection = db.getCollection(name)
        assert(coll != null)
        coll
    }

    override final def createDAOGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncDAOGroup[EntityType, IdType, IdType] = {
        val identityIdComposer = new IdentityIdComposer[IdType]
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            // special-case ObjectId to map Beaucatcher ObjectId to the org.bson version
            new EntityBObjectCasbahDAOGroup[EntityType, IdType, IdType, JavaObjectId](this,
                underlyingCollection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                CasbahBackend.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            new EntityBObjectCasbahDAOGroup[EntityType, IdType, IdType, IdType](this,
                underlyingCollection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                identityIdComposer)
        }
    }

    override def createDAOGroupWithoutEntity[IdType : Manifest](collectionName : String) : SyncDAOGroupWithoutEntity[IdType] = {
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            new BObjectCasbahDAOGroup[IdType, JavaObjectId](this,
                underlyingCollection(collectionName),
                CasbahBackend.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            val identityIdComposer = new IdentityIdComposer[IdType]
            new BObjectCasbahDAOGroup[IdType, IdType](this,
                underlyingCollection(collectionName),
                identityIdComposer)
        }
    }

    override final lazy val database = {
        new CasbahDatabase(this)
    }
}

private[casbah] object CasbahBackend {

    lazy val scalaToJavaObjectIdComposer = new IdComposer[ObjectId, JavaObjectId] {
        import j.JavaConversions._
        override def idIn(id : ObjectId) : JavaObjectId = id
        override def idOut(id : JavaObjectId) : ObjectId = id
    }

    val connections = new MongoConnectionStore[Mongo] {
        override def create(address : MongoConnectionAddress) = {
            val c = new Mongo(new MongoURI(address.url))
            // things are awfully race-prone without Safe, and you
            // don't get constraint violations for example
            c.setWriteConcern(WriteConcern.SAFE)
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
