package org.beaucatcher.mongo

/**
 * A DAO that backends to another DAO. The two may have different query, entity, and ID types.
 * This is an internal implementation class not exported from the library.
 */
private[beaucatcher] abstract trait ComposedSyncDAO[OuterQueryType, OuterEntityType, OuterIdType, OuterValueType, InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]
    extends SyncDAO[OuterQueryType, OuterEntityType, OuterIdType, OuterValueType] {

    protected val inner : SyncDAO[InnerQueryType, InnerEntityType, InnerIdType, InnerValueType]

    protected val queryComposer : QueryComposer[OuterQueryType, InnerQueryType]
    protected val entityComposer : EntityComposer[OuterEntityType, InnerEntityType]
    protected val idComposer : IdComposer[OuterIdType, InnerIdType]
    protected val valueComposer : ValueComposer[OuterValueType, InnerValueType]

    override def emptyQuery : OuterQueryType =
        queryOut(inner.emptyQuery)

    override def count(query : OuterQueryType, options : CountOptions) : Long =
        inner.count(queryIn(query), options)

    override def distinct(key : String, options : DistinctOptions[OuterQueryType]) : Seq[OuterValueType] =
        inner.distinct(key, options.convert(queryIn(_))) map { valueOut(_) }

    override def find(query : OuterQueryType, options : FindOptions) : Iterator[OuterEntityType] =
        inner.find(queryIn(query), options).map(entityOut(_))

    override def findOne(query : OuterQueryType, options : FindOneOptions) : Option[OuterEntityType] =
        entityOut(inner.findOne(queryIn(query), options))

    override def findOneById(id : OuterIdType, options : FindOneByIdOptions) : Option[OuterEntityType] =
        inner.findOneById(idIn(id), options) flatMap { e => Some(entityOut(e)) }

    override def findAndModify(query : OuterQueryType, update : Option[OuterQueryType], options : FindAndModifyOptions[OuterQueryType]) : Option[OuterEntityType] =
        entityOut(inner.findAndModify(queryIn(query), queryIn(update), options.convert(queryIn(_))))

    override def insert(o : OuterEntityType) : WriteResult =
        inner.insert(entityIn(o))

    override def update(query : OuterQueryType, modifier : OuterQueryType, options : UpdateOptions) : WriteResult =
        inner.update(queryIn(query), queryIn(modifier), options)

    override def remove(query : OuterQueryType) : WriteResult =
        inner.remove(queryIn(query))

    override def removeById(id : OuterIdType) : WriteResult =
        inner.removeById(idIn(id))

    /* These are all final because you should override the composers instead, these are
     * just here to save typing
     */
    final protected def queryIn(q : OuterQueryType) : InnerQueryType = queryComposer.queryIn(q)
    final protected def queryIn(q : Option[OuterQueryType]) : Option[InnerQueryType] =
        q map { queryIn(_) }
    final protected def queryOut(q : InnerQueryType) : OuterQueryType = queryComposer.queryOut(q)
    final protected def queryOut(q : Option[InnerQueryType]) : Option[OuterQueryType] =
        q map { queryOut(_) }
    final protected def entityIn(o : OuterEntityType) : InnerEntityType = entityComposer.entityIn(o)
    final protected def entityIn(o : Option[OuterEntityType]) : Option[InnerEntityType] =
        o map { entityIn(_) }
    final protected def entityOut(o : InnerEntityType) : OuterEntityType = entityComposer.entityOut(o)
    final protected def entityOut(o : Option[InnerEntityType]) : Option[OuterEntityType] =
        o map { entityOut(_) }
    final protected def idIn(id : OuterIdType) : InnerIdType = idComposer.idIn(id)
    final protected def idIn(id : Option[OuterIdType]) : Option[InnerIdType] =
        id map { idIn(_) }
    final protected def idOut(id : InnerIdType) : OuterIdType = idComposer.idOut(id)
    final protected def idOut(id : Option[InnerIdType]) : Option[OuterIdType] =
        id map { idOut(_) }
    final protected def valueIn(v : OuterValueType) : InnerValueType = valueComposer.valueIn(v)
    final protected def valueIn(v : Option[OuterValueType]) : Option[InnerValueType] =
        v map { valueIn(_) }
    final protected def valueOut(v : InnerValueType) : OuterValueType = valueComposer.valueOut(v)
    final protected def valueOut(v : Option[InnerValueType]) : Option[OuterValueType] =
        v map { valueOut(_) }
}
