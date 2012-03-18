package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb._
import org.bson.types.{ ObjectId => JavaObjectId, _ }
import org.joda.time.DateTime
import akka.actor.ActorSystem

/**
 * [[org.beaucatcher.jdriver.JavaDriver]] is final with a private constructor - there's no way to create one
 * directly. However, a [[org.beaucatcher.jdriver.JavaDriverBackendProvider]] exposes a [[org.beaucatcher.jdriver.JavaDriverBackend]] and
 * you may want to use `provider.backend.underlyingConnection`, `underlyingDB`, or `underlyingCollection`
 * to get at JavaDriver methods directly.
 */
final class JavaDriver private[jdriver] ()
    extends Driver {

    override final def createCollectionGroup[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : CollectionGroup[EntityType, IdType, IdType] = {
        val identityIdComposer = new IdentityIdComposer[IdType]
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            // special-case ObjectId to map Beaucatcher ObjectId to the org.bson version
            new EntityBObjectJavaDriverCollectionGroup[EntityType, IdType, IdType, JavaObjectId](this,
                collectionName,
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                JavaDriver.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            new EntityBObjectJavaDriverCollectionGroup[EntityType, IdType, IdType, IdType](this,
                collectionName,
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                identityIdComposer)
        }
    }

    override def createCollectionGroupWithoutEntity[IdType : Manifest](collectionName : String) : CollectionGroupWithoutEntity[IdType] = {
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            new BObjectJavaDriverCollectionGroup[IdType, JavaObjectId](this,
                collectionName,
                JavaDriver.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            val identityIdComposer = new IdentityIdComposer[IdType]
            new BObjectJavaDriverCollectionGroup[IdType, IdType](this,
                collectionName,
                identityIdComposer)
        }
    }

    def newContext(config : MongoConfig, system : ActorSystem) : Context = {
        new JavaDriverContext(this, config, system)
    }
}

private[jdriver] object JavaDriver {

    lazy val scalaToJavaObjectIdComposer = new IdComposer[ObjectId, JavaObjectId] {
        import JavaConversions._
        override def idIn(id : ObjectId) : JavaObjectId = id
        override def idOut(id : JavaObjectId) : ObjectId = id
    }

}

/**
 * Mix this trait into a subclass of [[org.beaucatcher.mongo.CollectionAccess]] to backend
 * the collection operations using JavaDriver
 */
trait JavaDriverProvider extends DriverProvider {

    override lazy val mongoDriver : JavaDriver = {
        new JavaDriver()
    }
}
