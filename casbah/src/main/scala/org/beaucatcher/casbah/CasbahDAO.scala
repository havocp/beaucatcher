package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
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

/* Mutable BSONObject/DBObject implementation used to save to MongoDB API */
private[casbah] class BObjectBSONObject extends BSONObject {
    import scalaj.collection.Implicits._

    private[this] var bvalue : BObject = BObject.empty

    def this(b : BObject) = {
        this()
        bvalue = b
    }

    /* BSONObject interface */
    override def containsField(s : String) : Boolean = {
        bvalue.contains(s)
    }
    override def containsKey(s : String) : Boolean = containsField(s)

    override def get(key : String) : AnyRef = {
        bvalue.get(key) match {
            case Some(bvalue) =>
                bvalue.unwrappedAsJava
            case None =>
                null
        }
    }

    override def keySet() : java.util.Set[String] = {
        bvalue.keySet.asJava
    }

    // returns previous value
    override def put(key : String, v : AnyRef) : AnyRef = {
        val previous = get(key)
        bvalue = bvalue + (key, BValue.wrap(v))
        previous
    }

    override def putAll(bsonObj : BSONObject) : Unit = {
        for { key <- bsonObj.keySet() }
            put(key, BValue.wrap(bsonObj.get(key)))
    }

    override def putAll(m : java.util.Map[_, _]) : Unit = {
        for { key <- m.keySet() }
            put(key.asInstanceOf[String], BValue.wrap(m.get(key)))
    }

    override def removeField(key : String) : AnyRef = {
        val previous = get(key)
        bvalue = bvalue - key
        previous
    }

    override def toMap() : java.util.Map[_, _] = {
        bvalue.unwrappedAsJava
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
    import org.beaucatcher.casbah.Implicits._

    override def queryIn(q : BObject) : DBObject = new BObjectDBObject(q)
    override def queryOut(q : DBObject) : BObject = q
}

private[casbah] class BObjectCasbahEntityComposer extends EntityComposer[BObject, DBObject] {
    import org.beaucatcher.casbah.Implicits._

    override def entityIn(o : BObject) : DBObject = new BObjectDBObject(o)
    override def entityOut(o : DBObject) : BObject = o
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
