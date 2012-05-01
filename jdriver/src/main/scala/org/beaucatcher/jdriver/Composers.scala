package org.beaucatcher.jdriver

// TODO we want to get rid of this composer concept

import org.beaucatcher.bson._

/**
 * Object which converts queries as they go from a Collection into or out of an "underlying" Collection.
 */
abstract trait QueryComposer[OuterQueryType, InnerQueryType] {
    def queryIn(q : OuterQueryType) : InnerQueryType
    def queryOut(q : InnerQueryType) : OuterQueryType
}

class IdentityQueryComposer[QueryType]
    extends QueryComposer[QueryType, QueryType] {
    override def queryIn(q : QueryType) = q
    override def queryOut(q : QueryType) = q
}

/**
 * Object which converts entities (document representations) as they go from a Collection into or out of an "underlying" Collection.
 */
abstract trait EntityComposer[OuterEntityType, InnerEntityType] {
    def entityIn(o : OuterEntityType) : InnerEntityType
    def entityOut(o : InnerEntityType) : OuterEntityType
}

class IdentityEntityComposer[EntityType]
    extends EntityComposer[EntityType, EntityType] {
    override def entityIn(o : EntityType) = o
    override def entityOut(o : EntityType) = o
}

/**
 * Object which converts IDs (usually the "_id" field value) as they go from a Collection into or out of an "underlying" Collection.
 */
abstract trait IdComposer[OuterIdType, InnerIdType] {
    def idIn(id : OuterIdType) : InnerIdType
    def idOut(id : InnerIdType) : OuterIdType
}

class IdentityIdComposer[IdType]
    extends IdComposer[IdType, IdType] {
    override def idIn(id : IdType) = id
    override def idOut(id : IdType) = id
}

/**
 * Object which converts values (the value part of the key-value pairs of a BSON object) as they go from a Collection into or out of an "underlying" Collection.
 */
abstract trait ValueComposer[OuterValueType, InnerValueType] {
    def valueIn(v : OuterValueType) : InnerValueType
    def valueOut(v : InnerValueType) : OuterValueType
}

class IdentityValueComposer[ValueType]
    extends ValueComposer[ValueType, ValueType] {
    override def valueIn(id : ValueType) = id
    override def valueOut(id : ValueType) = id
}
