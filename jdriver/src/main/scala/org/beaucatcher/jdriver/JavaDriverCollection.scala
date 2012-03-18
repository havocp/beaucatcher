package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.mongo.wire._
import org.bson.BSONObject
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, _ }

import JavaConversions._

/**
 * Base trait that chains SyncCollection methods to a JavaDriver collection, which must be provided
 * by a subclass of this trait.
 */
abstract trait JavaDriverSyncCollection[IdType <: Any] extends SyncCollection[DBObject, DBObject, IdType, Any] {

    override private[beaucatcher] def context : JavaDriverContext

    protected def collection : DBCollection

    private implicit def fields2dbobject(fields : Fields) : DBObject = {
        val builder = new BasicDBObjectBuilder()
        for (i <- fields.included) {
            builder.add(i, 1)
        }
        for (e <- fields.excluded) {
            builder.add(e, 0)
        }
        builder.get()
    }

    override def name : String = collection.getName()

    override def emptyQuery : DBObject = new BasicDBObject() // not immutable, so we always make a new one

    override def entityToUpsertableObject(entity : DBObject) : DBObject = {
        // maybe we should copy this since it isn't immutable :-/
        // however at the moment I don't think the app can get a reference to it,
        // at least not easily, so copying would just be paranoia
        entity
    }

    override def entityToModifierObject(entity : DBObject) : DBObject = {
        entityToUpsertableObject(entity)
    }

    private def withQueryFlags[R](maybeOverrideFlags : Option[Set[QueryFlag]])(body : => R) : R = {
        if (maybeOverrideFlags.isDefined) {
            // FIXME this is outrageously unthreadsafe but I'm not sure how to
            // fix it given how JavaDriver works. It doesn't have API to override options
            // for anything other than find() it looks like
            // so for now just always throw an exception
            val saved = collection.getOptions()
            collection.resetOptions()
            collection.addOption(maybeOverrideFlags.get)

            val result = body

            collection.resetOptions()
            collection.addOption(saved)
            throw new UnsupportedOperationException("JavaDriver backend can't override query options on this operation")
            //result
        } else {
            body
        }
    }

    override def entityToUpdateQuery(entity : DBObject) : DBObject = {
        if (!entity.containsField("_id"))
            throw new IllegalArgumentException("Object is missing an _id field, can't save() or whatever you are doing")
        val obj = new BasicDBObject()
        obj.put("_id", entity.get("_id"))
        obj
    }

    override def count(query : DBObject, options : CountOptions) : Long = {
        withQueryFlags(options.overrideQueryFlags) {
            val fieldsQuery = if (options.fields.isDefined) options.fields.get : DBObject else emptyQuery
            if (options.limit.isDefined || options.skip.isDefined)
                collection.getCount(query, fieldsQuery, options.limit.getOrElse(0), options.skip.getOrElse(0))
            else
                collection.getCount(query, fieldsQuery)
        }
    }

    override def distinct(key : String, options : DistinctOptions[DBObject]) : Iterator[Any] = {
        import scala.collection.JavaConverters._

        withQueryFlags(options.overrideQueryFlags) {
            if (options.query.isDefined)
                collection.distinct(key, options.query.get).asScala.iterator
            else
                collection.distinct(key).asScala.iterator
        }
    }

    // adapter from DBCursor to scala Iterator
    private class CursorIterator(cursor : DBCursor) extends Iterator[DBObject] {
        override def next() : DBObject = {
            cursor.next()
        }

        override def hasNext() : Boolean = {
            cursor.hasNext()
        }
    }

    override def find(query : DBObject, options : FindOptions) : Iterator[DBObject] = {
        import scala.collection.JavaConverters._

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
            cursor.setOptions(options.overrideQueryFlags.get)
        }

        new CursorIterator(cursor)
    }

    override def findOne(query : DBObject, options : FindOneOptions) : Option[DBObject] = {
        withQueryFlags(options.overrideQueryFlags) {
            if (options.fields.isDefined) {
                Option(collection.findOne(query, options.fields.get))
            } else {
                Option(collection.findOne(query))
            }
        }
    }

    override def findOneById(id : IdType, options : FindOneByIdOptions) : Option[DBObject] = {
        val query = new BasicDBObject()
        query.put("_id", id)
        findOne(query, FindOneOptions(options.fields, options.overrideQueryFlags))
    }

    override def findAndModify(query : DBObject, update : Option[DBObject], options : FindAndModifyOptions[DBObject]) : Option[DBObject] = {
        if (options.flags.contains(FindAndModifyRemove)) {
            if (update.isDefined)
                throw new IllegalArgumentException("Does not make sense to provide a replacement or modifier object to findAndModify with remove flag")
            Option(collection.findAndRemove(query))
        } else if (!update.isDefined) {
            throw new IllegalArgumentException("Must provide a replacement or modifier object to findAndModify")
        } else if (options.flags.isEmpty && !options.fields.isDefined) {
            if (options.sort.isDefined)
                Option(collection.findAndModify(query, options.sort.get, update.get))
            else
                Option(collection.findAndModify(query, update.get))
        } else {
            Option(collection.findAndModify(query,
                // getOrElse mixes poorly with implicit conversion from Fields
                if (options.fields.isDefined) { options.fields.get : DBObject } else { emptyQuery },
                options.sort.getOrElse(emptyQuery),
                false, // remove
                update.get,
                options.flags.contains(FindAndModifyNew),
                options.flags.contains(FindAndModifyUpsert)))
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
        val obj = new BasicDBObject()
        obj.put("_id", id)
        collection.remove(obj)
    }

    override def ensureIndex(keys : DBObject, options : IndexOptions) : WriteResult = {
        throw new BugInSomethingMongoException("ensureIndex() should be implemented on an outer wrapper Collection and not make it here")
    }

    override def dropIndex(indexName : String) : CommandResult = {
        throw new BugInSomethingMongoException("dropIndex() should be implemented on an outer wrapper Collection and not make it here")
    }
}

private[jdriver] class BObjectJavaDriverQueryComposer extends QueryComposer[BObject, DBObject] {
    import org.beaucatcher.jdriver.Implicits._

    override def queryIn(q : BObject) : DBObject = new BObjectDBObject(q)
    override def queryOut(q : DBObject) : BObject = q
}

private[jdriver] class BObjectJavaDriverEntityComposer extends EntityComposer[BObject, DBObject] {
    import org.beaucatcher.jdriver.Implicits._

    override def entityIn(o : BObject) : DBObject = new BObjectDBObject(o)
    override def entityOut(o : DBObject) : BObject = o
}

/**
 * A BObject Collection that specifically backends to a JavaDriver Collection.
 * Subclass would provide the backend and could override the in/out type converters.
 */
private[jdriver] abstract trait BObjectJavaDriverSyncCollection[OuterIdType, InnerIdType]
    extends BObjectComposedSyncCollection[OuterIdType, DBObject, DBObject, InnerIdType, Any] {
    override protected val inner : JavaDriverSyncCollection[InnerIdType]

    override protected val queryComposer : QueryComposer[BObject, DBObject]
    override protected val entityComposer : EntityComposer[BObject, DBObject]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
    override protected val valueComposer : ValueComposer[BValue, Any]
    override protected val exceptionMapper = jdriverExceptionMapper
}
