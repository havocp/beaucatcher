package org.beaucatcher.mongo

import org.beaucatcher.bson._
import com.mongodb.WriteResult

/**
 * A Sync DAO parameterized to work with BObject
 */
abstract trait BObjectSyncDAO[IdType] extends SyncDAO[BObject, BObject, IdType, BValue] {
}

/** A BObject DAO that backends to another DAO. This is an internal implementation class not exported from the library. */
private[beaucatcher] abstract trait BObjectComposedSyncDAO[OuterIdType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]
    extends BObjectSyncDAO[OuterIdType]
    with ComposedSyncDAO[BObject, BObject, OuterIdType, BValue, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType] {

    override def entityToModifierObject(entity : BObject) : BObject = {
        entity
    }
}
