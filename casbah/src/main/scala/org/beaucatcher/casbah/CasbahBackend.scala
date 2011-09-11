package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb.casbah.MongoURI
import com.mongodb.casbah.Imports.{ ObjectId => JavaObjectId, _ }
import com.mongodb.casbah.commons.conversions.scala._
import org.joda.time.DateTime

private[casbah] class CasbahBackend(private val config : MongoConfig)
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

    override lazy val backend : MongoBackend = {
        require(mongoConfig != null)
        new CasbahBackend(mongoConfig)
    }
}
