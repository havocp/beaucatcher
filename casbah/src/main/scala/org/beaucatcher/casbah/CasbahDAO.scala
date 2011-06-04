package com.ometer.casbah

import com.ometer.bson.BsonAST._
import com.ometer.mongo._
import com.mongodb.WriteResult
import com.mongodb.casbah.MongoCollection
import com.mongodb.CommandResult
import com.mongodb.DBObject

/**
 * Base trait that chains SyncDAO methods to a Casbah collection, which must be provided
 * by a subclass of this trait.
 */
abstract trait CasbahSyncDAO[IdType <: Any] extends SyncDAO[DBObject, DBObject, IdType] {
    protected def collection : MongoCollection

    override def find[A <% DBObject](ref : A) : Iterator[DBObject] = {
        collection.find(ref)
    }

    override def findOne[A <% DBObject](t : A) : Option[DBObject] = {
        collection.findOne(t)
    }

    override def findOneByID(id : IdType) : Option[DBObject] = {
        collection.findOneByID(id.asInstanceOf[AnyRef])
    }

    override def findAndModify[A <% DBObject](q : A, t : DBObject) : Option[DBObject] = {
        collection.findAndModify(q, t)
    }

    override def save(t : DBObject) : WriteResult = {
        collection.save(t)
    }

    override def insert(t : DBObject) : WriteResult = {
        collection.insert(t)
    }

    override def update[A <% DBObject](q : A, o : DBObject) : WriteResult = {
        collection.update(q, o)
    }

    override def remove(t : DBObject) : WriteResult = {
        collection.remove(t)
    }
}

/**
 * adds DBObject extensions to BSONObject.
 * This is an internal implementation class not exported by the library.
 */
private[casbah] class BObjectDBObject(b : BObject) extends BObjectBSONObject(b) with DBObject {
    private[this] var isPartial : Boolean = false

    def this() = {
        this(BObject.empty)
    }

    override def isPartialObject() : Boolean = isPartial

    override def markAsPartialObject() : Unit = {
        isPartial = true
    }
}

private[casbah] class BObjectCasbahQueryComposer extends QueryComposer[BObject, DBObject] {
    override def queryIn(q : BObject) : DBObject = new BObjectDBObject(q)
    override def queryOut(q : DBObject) : BObject = BObject(q)
}

private[casbah] class BObjectCasbahEntityComposer extends EntityComposer[BObject, DBObject] {
    override def entityIn(o : BObject) : DBObject = new BObjectDBObject(o)
    override def entityOut(o : DBObject) : BObject = BObject(o)
}

/**
 * A BObject DAO that specifically backends to a Casbah DAO.
 * Subclass would provide the backend and could override the in/out type converters.
 */
private[casbah] abstract trait BObjectCasbahSyncDAO[OuterIdType, InnerIdType]
    extends BObjectComposedSyncDAO[OuterIdType, DBObject, DBObject, InnerIdType] {
    override protected val backend : CasbahSyncDAO[InnerIdType]

    override protected val queryComposer : QueryComposer[BObject, DBObject]
    override protected val entityComposer : EntityComposer[BObject, DBObject]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
}
