package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import com.mongodb._
import org.bson.types.{ ObjectId => JavaObjectId, _ }
import org.joda.time.DateTime
import akka.actor.ActorSystem

/**
 * [[org.beaucatcher.jdriver.JavaDriver]] is final with a private constructor - there's no way to create one
 * directly. If you're creating an [[org.beaucatcher.mongo.CollectionAccess]] object then mix in
 * [[org.beaucatcher.jdriver.JavaDriverProvider]]. Otherwise the [[org.beaucatcher.jdriver.JavaDriver]]
 * companion object has a field called `instance` with a driver instance.
 */
final class JavaDriver private[jdriver] ()
    extends Driver {

    override final def createCollectionFactory[EntityType <: AnyRef : Manifest, IdType : Manifest](collectionName : String,
        caseClassBObjectQueryComposer : QueryComposer[BObject, BObject],
        caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]) : CollectionFactory[EntityType, IdType, IdType] = {
        val identityIdComposer = new IdentityIdComposer[IdType]
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            // special-case ObjectId to map Beaucatcher ObjectId to the org.bson version
            new EntityBObjectJavaDriverCollectionFactory[EntityType, IdType, IdType, JavaObjectId](this,
                collectionName,
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                JavaDriver.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            new EntityBObjectJavaDriverCollectionFactory[EntityType, IdType, IdType, IdType](this,
                collectionName,
                caseClassBObjectQueryComposer,
                caseClassBObjectEntityComposer,
                identityIdComposer,
                identityIdComposer)
        }
    }

    override def createCollectionFactoryWithoutEntity[IdType : Manifest](collectionName : String) : CollectionFactoryWithoutEntity[IdType] = {
        val idManifest = manifest[IdType]
        if (idManifest <:< manifest[ObjectId]) {
            new BObjectJavaDriverCollectionFactory[IdType, JavaObjectId](this,
                collectionName,
                JavaDriver.scalaToJavaObjectIdComposer.asInstanceOf[IdComposer[IdType, JavaObjectId]])
        } else {
            val identityIdComposer = new IdentityIdComposer[IdType]
            new BObjectJavaDriverCollectionFactory[IdType, IdType](this,
                collectionName,
                identityIdComposer)
        }
    }

    def newContext(config : MongoConfig, system : ActorSystem) : Context = {
        new JavaDriverContext(this, config, system)
    }
}

object JavaDriver {

    private[jdriver] lazy val scalaToJavaObjectIdComposer = new IdComposer[ObjectId, JavaObjectId] {
        import JavaConversions._
        override def idIn(id : ObjectId) : JavaObjectId = id
        override def idOut(id : JavaObjectId) : ObjectId = id
    }

    lazy val instance = new JavaDriver()
}

/**
 * Mix this trait into a subclass of [[org.beaucatcher.mongo.CollectionAccess]] to backend
 * the collection operations using JavaDriver
 */
trait JavaDriverProvider extends DriverProvider {

    override def mongoDriver : JavaDriver =
        JavaDriver.instance
}
