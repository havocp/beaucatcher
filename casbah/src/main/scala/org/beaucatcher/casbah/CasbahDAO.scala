package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import com.mongodb.WriteResult
import com.mongodb.casbah.MongoCollection
import com.mongodb.CommandResult
import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject

/**
 * Base trait that chains SyncDAO methods to a Casbah collection, which must be provided
 * by a subclass of this trait.
 */
abstract trait CasbahSyncDAO[IdType <: Any] extends SyncDAO[DBObject, DBObject, IdType, Any] {
    protected def collection : MongoCollection

    private implicit def fields2dbobject(fields : Fields) : DBObject = {
        val builder = MongoDBObject.newBuilder
        for (i <- fields.included) {
            builder += (i -> 1)
        }
        for (e <- fields.excluded) {
            builder += (e -> 0)
        }
        builder.result
    }

    override def emptyQuery : DBObject = MongoDBObject() // not immutable, so we always make a new one

    override def entityToModifierObject(entity : DBObject) : DBObject = {
        entity
    }

    override def count(query : DBObject, options : CountOptions) : Long = {
        if (options.fields.isDefined)
            collection.count(query, options.fields.get)
        else
            collection.count(query)
    }

    override def distinct(key : String, options : DistinctOptions[DBObject]) : Seq[Any] = {
        if (options.query.isDefined)
            collection.distinct(key, options.query.get)
        else
            collection.distinct(key)
    }

    override def find(query : DBObject, options : FindOptions) : Iterator[DBObject] = {
        if (options.fields.isDefined && (options.batchSize.isDefined || options.numToSkip.isDefined)) {
            collection.find(query, options.fields.get,
                options.numToSkip.getOrElse(0),
                options.batchSize.getOrElse(-1))
        } else if (options.fields.isDefined) {
            collection.find(query, options.fields.get)
        } else {
            collection.find(query)
        }
    }

    override def findOne(query : DBObject, options : FindOneOptions) : Option[DBObject] = {
        if (options.fields.isDefined) {
            collection.findOne(query, options.fields.get)
        } else {
            collection.findOne(query)
        }
    }

    override def findOneById(id : IdType, options : FindOneByIdOptions) : Option[DBObject] = {
        if (options.fields.isDefined) {
            collection.findOneByID(id.asInstanceOf[AnyRef], options.fields.get)
        } else {
            collection.findOneByID(id.asInstanceOf[AnyRef])
        }
    }

    override def findAndModify(query : DBObject, update : Option[DBObject], options : FindAndModifyOptions[DBObject]) : Option[DBObject] = {
        if (options.flags.contains(FindAndModifyRemove)) {
            if (update.isDefined)
                throw new IllegalArgumentException("Does not make sense to provide a replacement or modifier object to findAndModify with remove flag")
            collection.findAndRemove(query)
        } else if (!update.isDefined) {
            throw new IllegalArgumentException("Must provide a replacement or modifier object to findAndModify")
        } else if (options.flags.isEmpty && !options.fields.isDefined) {
            if (options.sort.isDefined)
                collection.findAndModify(query, options.sort.get, update.get)
            else
                collection.findAndModify(query, update.get)
        } else {
            collection.findAndModify(query,
                // getOrElse mixes poorly with implicit conversion from Fields
                if (options.fields.isDefined) { options.fields.get : DBObject } else { emptyQuery },
                options.sort.getOrElse(emptyQuery),
                false, // remove
                update.get,
                options.flags.contains(FindAndModifyNew),
                options.flags.contains(FindAndModifyUpsert))
        }
    }

    override def save(o : DBObject) : WriteResult = {
        collection.save(o)
    }

    override def insert(o : DBObject) : WriteResult = {
        collection.insert(o)
    }

    override def update(query : DBObject, modifier : DBObject, options : UpdateOptions) : WriteResult = {
        collection.update(query, modifier, options.flags.contains(UpdateUpsert), options.flags.contains(UpdateMulti))
    }

    override def remove(query : DBObject) : WriteResult = {
        collection.remove(query)
    }

    override def removeById(id : IdType) : WriteResult = {
        collection.remove(MongoDBObject("_id" -> id))
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
    extends BObjectComposedSyncDAO[OuterIdType, DBObject, DBObject, InnerIdType, Any] {
    override protected val backend : CasbahSyncDAO[InnerIdType]

    override protected val queryComposer : QueryComposer[BObject, DBObject]
    override protected val entityComposer : EntityComposer[BObject, DBObject]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
    override protected val valueComposer : ValueComposer[BValue, Any]
}
