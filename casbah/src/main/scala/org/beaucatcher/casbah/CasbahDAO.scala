package org.beaucatcher.casbah

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.bson.BSONObject
import com.mongodb.casbah.MongoCollection
import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.Bytes

import j.JavaConversions._

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

    override def entityToUpsertableObject(entity : DBObject) : DBObject = {
        // maybe we should copy this since it isn't immutable :-/
        // however at the moment I don't think the app can get a reference to it,
        // at least not easily, so copying would just be paranoia
        entity
    }

    override def entityToModifierObject(entity : DBObject) : DBObject = {
        entityToUpsertableObject(entity)
    }

    private def queryFlagsAsInt(flags : Set[QueryFlag]) : Int = {
        var i = 0
        for (f <- flags) {
            val o = f match {
                case QueryAwaitData => Bytes.QUERYOPTION_AWAITDATA
                case QueryExhaust => Bytes.QUERYOPTION_EXHAUST
                case QueryNoTimeout => Bytes.QUERYOPTION_NOTIMEOUT
                case QueryOpLogReplay => Bytes.QUERYOPTION_OPLOGREPLAY
                case QuerySlaveOk => Bytes.QUERYOPTION_SLAVEOK
                case QueryTailable => Bytes.QUERYOPTION_TAILABLE
            }
            i |= o
        }
        i
    }

    private def withQueryFlags[R](maybeOverrideFlags : Option[Set[QueryFlag]])(body : => R) : R = {
        if (maybeOverrideFlags.isDefined) {
            // FIXME this is outrageously unthreadsafe but I'm not sure how to
            // fix it given how Casbah works. It doesn't have API to override options
            // for anything other than find() it looks like
            // so for now just always throw an exception
            val saved = collection.getOptions()
            collection.resetOptions()
            collection.addOption(queryFlagsAsInt(maybeOverrideFlags.get))

            val result = body

            collection.resetOptions()
            collection.addOption(saved)
            throw new UnsupportedOperationException("Casbah backend can't override query options on this operation")
            //result
        } else {
            body
        }
    }

    override def entityToUpdateQuery(entity : DBObject) : DBObject = {
        if (!entity.containsField("_id"))
            throw new IllegalArgumentException("Object is missing an _id field, can't save() or whatever you are doing")
        MongoDBObject("_id" -> entity.get("_id"))
    }

    override def count(query : DBObject, options : CountOptions) : Long = {
        withQueryFlags(options.overrideQueryFlags) {
            val fieldsQuery = if (options.fields.isDefined) options.fields.get : DBObject else emptyQuery
            if (options.limit.isDefined || options.skip.isDefined)
                collection.getCount(query, fieldsQuery, options.limit.getOrElse(0), options.skip.getOrElse(0))
            else
                collection.count(query, fieldsQuery)
        }
    }

    override def distinct(key : String, options : DistinctOptions[DBObject]) : Seq[Any] = {
        withQueryFlags(options.overrideQueryFlags) {
            if (options.query.isDefined)
                collection.distinct(key, options.query.get)
            else
                collection.distinct(key)
        }
    }

    override def find(query : DBObject, options : FindOptions) : Iterator[DBObject] = {
        val cursor = {
            if (options.fields.isDefined) {
                collection.find(query, options.fields.get)
            } else {
                collection.find(query)
            }
        }
        if (options.skip.isDefined)
            cursor.skip(options.skip.get.intValue)
        if (options.limit.isDefined)
            cursor.limit(options.limit.get.intValue)
        if (options.batchSize.isDefined)
            cursor.batchSize(options.batchSize.get.intValue)
        if (options.overrideQueryFlags.isDefined) {
            cursor.options = queryFlagsAsInt(options.overrideQueryFlags.get)
        }

        cursor
    }

    override def findOne(query : DBObject, options : FindOneOptions) : Option[DBObject] = {
        withQueryFlags(options.overrideQueryFlags) {
            if (options.fields.isDefined) {
                collection.findOne(query, options.fields.get)
            } else {
                collection.findOne(query)
            }
        }
    }

    override def findOneById(id : IdType, options : FindOneByIdOptions) : Option[DBObject] = {
        withQueryFlags(options.overrideQueryFlags) {
            if (options.fields.isDefined) {
                collection.findOneByID(id.asInstanceOf[AnyRef], options.fields.get)
            } else {
                collection.findOneByID(id.asInstanceOf[AnyRef])
            }
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

    override def insert(o : DBObject) : WriteResult = {
        import Implicits._
        collection.insert(o)
    }

    override def update(query : DBObject, modifier : DBObject, options : UpdateOptions) : WriteResult = {
        import Implicits._
        collection.update(query, modifier, options.flags.contains(UpdateUpsert), options.flags.contains(UpdateMulti))
    }

    override def remove(query : DBObject) : WriteResult = {
        import Implicits._
        collection.remove(query)
    }

    override def removeById(id : IdType) : WriteResult = {
        import Implicits._
        collection.remove(MongoDBObject("_id" -> id))
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
