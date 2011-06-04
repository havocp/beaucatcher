package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * A Sync DAO parameterized to work with BObject
 */
abstract trait BObjectSyncDAO[IdType] extends SyncDAO[BObject, BObject, IdType] {

}

/** A BObject DAO that backends to another DAO. This is an internal implementation class not exported from the library. */
private[beaucatcher] abstract trait BObjectComposedSyncDAO[OuterIdType, InnerQueryType, InnerEntityType, InnerIdType]
    extends BObjectSyncDAO[OuterIdType]
    with ComposedSyncDAO[BObject, BObject, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType] {
}
