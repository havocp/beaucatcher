package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.bson.Implicits._

/**
 * A Sync Collection parameterized to work with BObject
 */
abstract trait BObjectSyncCollection[IdType] extends SyncCollection[BObject, BObject, IdType, BValue] {
}

/** A BObject Collection that backends to another Collection. This is an internal implementation class not exported from the library. */
private[beaucatcher] abstract trait BObjectComposedSyncCollection[OuterIdType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]
    extends BObjectSyncCollection[OuterIdType]
    with ComposedSyncCollection[BObject, BObject, OuterIdType, BValue, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType] {

    override def entityToUpsertableObject(entity : BObject) : BObject = {
        entity
    }

    override def entityToModifierObject(entity : BObject) : BObject = {
        // not allowed to change the _id
        entity - "_id"
    }

    override def entityToUpdateQuery(entity : BObject) : BObject = {
        BObject("_id" -> entity.getOrElse("_id", throw new IllegalArgumentException("only objects with an _id field work here")))
    }

    override def ensureIndex(keys : BObject, options : IndexOptions) : WriteResult = {
        val builder = BObject.newBuilder
        val indexName = options.name.getOrElse(defaultIndexName(keys))
        builder += ("name" -> indexName)
        builder += ("ns" -> fullName)
        options.v foreach { v => builder += ("v" -> v) }
        for (flag <- options.flags) {
            flag match {
                case IndexUnique => builder += ("unique" -> true)
                case IndexBackground => builder += ("background" -> true)
                case IndexDropDups => builder += ("dropDups" -> true)
                case IndexSparse => builder += ("sparse" -> true)
            }
        }
        builder += ("key" -> keys)
        val query = builder.result
        database.system.indexes.sync[BObject].insert(query)
    }

    override def dropIndex(indexName : String) : CommandResult = {
        database.sync.command(BObject("deleteIndexes" -> name, "index" -> indexName))
    }
}
