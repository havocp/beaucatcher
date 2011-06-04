package com.ometer.mongo

import com.mongodb.WriteResult

/**
 * A DAO that backends to another DAO. The two may have different query, entity, and ID types.
 * This is an internal implementation class not exported from the library.
 */
private[ometer] abstract trait ComposedSyncDAO[OuterQueryType, OuterEntityType, OuterIdType, InnerQueryType, InnerEntityType, InnerIdType]
    extends SyncDAO[OuterQueryType, OuterEntityType, OuterIdType] {

    protected val backend : SyncDAO[InnerQueryType, InnerEntityType, InnerIdType]

    protected val queryComposer : QueryComposer[OuterQueryType, InnerQueryType]
    protected val entityComposer : EntityComposer[OuterEntityType, InnerEntityType]
    protected val idComposer : IdComposer[OuterIdType, InnerIdType]

    override def find[A <% OuterQueryType](ref : A) : Iterator[OuterEntityType] = {
        backend.find(queryIn(ref)).map(entityOut(_))
    }

    override def findOne[A <% OuterQueryType](t : A) : Option[OuterEntityType] = {
        backend.findOne(queryIn(t)).map(entityOut(_))
    }

    override def findOneByID(id : OuterIdType) : Option[OuterEntityType] = {
        backend.findOneByID(idIn(id)).map(entityOut(_))
    }

    override def findAndModify[A <% OuterQueryType](q : A, t : OuterEntityType) : Option[OuterEntityType] = {
        backend.findAndModify(queryIn(q), entityIn(t)).map(entityOut(_))
    }

    override def save(t : OuterEntityType) : WriteResult = {
        backend.save(entityIn(t))
    }

    override def insert(t : OuterEntityType) : WriteResult = {
        backend.insert(entityIn(t))
    }

    override def update[A <% OuterQueryType](q : A, o : OuterEntityType) : WriteResult = {
        backend.update(queryIn(q), entityIn(o))
    }

    override def remove(t : OuterEntityType) : WriteResult = {
        backend.remove(entityIn(t))
    }

    /* These are all final because you should override the composers instead, these are
     * just here to save typing
     */
    final protected def queryIn(q : OuterQueryType) : InnerQueryType = queryComposer.queryIn(q)
    final protected def queryOut(q : InnerQueryType) : OuterQueryType = queryComposer.queryOut(q)
    final protected def entityIn(o : OuterEntityType) : InnerEntityType = entityComposer.entityIn(o)
    final protected def entityOut(o : InnerEntityType) : OuterEntityType = entityComposer.entityOut(o)
    final protected def idIn(id : OuterIdType) : InnerIdType = idComposer.idIn(id)
    final protected def idOut(id : InnerIdType) : OuterIdType = idComposer.idOut(id)
}
