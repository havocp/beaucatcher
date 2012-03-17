package org.beaucatcher.mongo

import org.beaucatcher.bson.ClassAnalysis
import org.beaucatcher.bson._

/**
 * The general type of a case class Collection that backends to another Collection.
 * This is an internal implementation class not exported from the library.
 */
private[beaucatcher] abstract trait CaseClassComposedSyncCollection[OuterQueryType, EntityType <: Product, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]
    extends CaseClassSyncCollection[OuterQueryType, EntityType, OuterIdType]
    with EntityComposedSyncCollection[OuterQueryType, EntityType, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType] {
}

private[beaucatcher] class CaseClassBObjectEntityComposer[EntityType <: Product : Manifest]
    extends EntityComposer[EntityType, BObject] {

    private lazy val analysis : ClassAnalysis[EntityType] = {
        new ClassAnalysis[EntityType](manifest[EntityType].erasure.asInstanceOf[Class[EntityType]])
    }

    override def entityIn(o : EntityType) : BObject = {
        BObject(analysis.asMap(o).map(kv => (kv._1, BValue.wrap(kv._2))))
    }

    override def entityOut(o : BObject) : EntityType = {
        analysis.fromMap(o.unwrapped)
    }
}

/**
 * A case class Collection that specifically backends to a BObject Collection and uses BObject
 * for queries.
 * Subclass would provide the backend and could override the in/out type converters.
 * This is an internal implementation class not exported from the library.
 */
private[beaucatcher] abstract trait CaseClassBObjectSyncCollection[EntityType <: Product, OuterIdType, InnerIdType]
    extends CaseClassComposedSyncCollection[BObject, EntityType, OuterIdType, BObject, BObject, InnerIdType, BValue]
    with EntityBObjectSyncCollection[EntityType, OuterIdType, InnerIdType] {

}
