package com.ometer.mongo

import com.ometer.bson.BsonAST._

/**
 * A Sync DAO parameterized to work with BObject
 */
abstract trait BObjectSyncDAO[IdType] extends SyncDAO[BObject, BObject, IdType] {

}

/** A BObject DAO that backends to another DAO. This is an internal implementation class not exported from the library. */
private[ometer] abstract trait BObjectComposedSyncDAO[OuterIdType, InnerQueryType, InnerEntityType, InnerIdType]
    extends BObjectSyncDAO[OuterIdType]
    with ComposedSyncDAO[BObject, BObject, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType] {
}
