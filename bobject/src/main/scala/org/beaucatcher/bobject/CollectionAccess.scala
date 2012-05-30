package org.beaucatcher.bobject

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.caseclass._

abstract class CollectionAccessWithEntityBObject[IdType]()(implicit val idEncoder: IdEncoder[IdType])
    extends CollectionAccessWithOneEntityType[BObject, BObject, IdType, BValue] {

    override val firstCodecSet = CollectionCodecSetBObject[IdType]()(idEncoder)
}

object CollectionAccessWithEntityBObject {
    def apply[IdType: IdEncoder](name: String,
        migrateCallback: (CollectionAccessWithEntityBObject[IdType], Context) => Unit) = {
        new CollectionAccessWithEntityBObject[IdType] {
            override val collectionName = name

            override def migrate(implicit context: Context) = migrateCallback(this, context)
        }
    }
}

abstract class CollectionAccessWithEntityBObjectIdObjectId
    extends CollectionAccessWithEntityBObject[ObjectId]()(IdEncoders.objectIdIdEncoder) {

}

// TODO flip so that the first entity type (which is assumed if none specified)
// is EntityType rather than BObject.
abstract class CollectionAccessWithEntitiesBObjectOrCaseClass[EntityType <: Product, IdType]()(implicit entityManifest: Manifest[EntityType],
    idEncoder: IdEncoder[IdType])
    extends CollectionAccessWithTwoEntityTypes[BObject, IdType, BObject, BValue, EntityType, Any] {

    override val firstCodecSet = CollectionCodecSetBObject[IdType]()(idEncoder)
    override val secondCodecSet = CollectionCodecSetCaseClass[BObject, EntityType, IdType]()(entityManifest, BObjectCodecs.bobjectQueryEncoder, BObjectCodecs.bobjectModifierEncoder, idEncoder)
}

abstract class CollectionAccessWithEntitiesBObjectOrCaseClassIdObjectId[EntityType <: Product]()(implicit entityManifest: Manifest[EntityType])
    extends CollectionAccessWithEntitiesBObjectOrCaseClass[EntityType, ObjectId]()(entityManifest, IdEncoders.objectIdIdEncoder) {

}
