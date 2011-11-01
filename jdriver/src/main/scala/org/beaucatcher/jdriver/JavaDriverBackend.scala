package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb._
import org.bson.types.{ ObjectId => JavaObjectId, _ }
import org.joda.time.DateTime

/**
 * [[org.beaucatcher.jdriver.JavaDriverBackend]] is final with a private constructor - there's no way to create one
 * directly. However, a [[org.beaucatcher.jdriver.JavaDriverBackendProvider]] exposes a [[org.beaucatcher.jdriver.JavaDriverBackend]] and
 * you may want to use `provider.backend.underlyingConnection`, `underlyingDB`, or `underlyingCollection`
 * to get at JavaDriver methods directly.
 */
final class JavaDriverBackend private[jdriver] (override val config : MongoConfig)
    extends MongoBackend {

    private lazy val jdriverURI = new MongoURI(config.url)
    private lazy val connection = JavaDriverBackend.connections.ensure(MongoConnectionAddress(config.url))

    override type ConnectionType = Mongo
    override type DatabaseType = DB
    override type CollectionType = DBCollection

    override def underlyingConnection : Mongo = connection
    override def underlyingDatabase : DB = connection.getDB(jdriverURI.getDatabase())
    override def underlyingCollection(name : String) : DBCollection = {
        if (name == null)
            throw new IllegalArgumentException("null collection name")
        val db : DB = underlyingDatabase
        assert(db != null)
        val coll : DBCollection = db.getCollection(name)
        assert(coll != null)
        coll
    }

    override final def createCollectionGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : SyncCollectionGroup[EntityType, IdType, IdType] = {
        val identityIdComposer = new IdentityIdComposer[IdType]
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            // special-case ObjectId to map Beaucatcher ObjectId to the org.bson version
            new EntityBObjectJavaDriverCollectionGroup[EntityType, IdType, IdType, JavaObjectId](this,
                underlyingCollection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                JavaDriverBackend.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            new EntityBObjectJavaDriverCollectionGroup[EntityType, IdType, IdType, IdType](this,
                underlyingCollection(collectionName),
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                identityIdComposer)
        }
    }

    override def createCollectionGroupWithoutEntity[IdType : Manifest](collectionName : String) : SyncCollectionGroupWithoutEntity[IdType] = {
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            new BObjectJavaDriverCollectionGroup[IdType, JavaObjectId](this,
                underlyingCollection(collectionName),
                JavaDriverBackend.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            val identityIdComposer = new IdentityIdComposer[IdType]
            new BObjectJavaDriverCollectionGroup[IdType, IdType](this,
                underlyingCollection(collectionName),
                identityIdComposer)
        }
    }

    override final lazy val database = {
        new JavaDriverDatabase(this)
    }
}

private[jdriver] object JavaDriverBackend {

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
 * the collection operations using JavaDriver
 */
trait JavaDriverBackendProvider extends MongoBackendProvider {
    self : MongoConfigProvider =>

    override lazy val backend : JavaDriverBackend = {
        require(mongoConfig != null)
        new JavaDriverBackend(mongoConfig)
    }
}
