package org.beaucatcher.mongo.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._
import org.bson.BSONObject
import com.mongodb.{ WriteResult => JavaWriteResult, CommandResult => JavaCommandResult, MongoException => JavaMongoException, _ }
import org.bson.BSONException

private[jdriver] final class JavaDriverSyncCollection(val collection : DBCollection, override val context : JavaDriverContext) extends SyncDriverCollection {

    private def withExceptionsMapped[A](body : => A) : A = {
        try {
            body
        } catch {
            case ex : JavaMongoException.DuplicateKey => throw new DuplicateKeyMongoException("Java driver: " + ex.getMessage, ex)
            case ex : JavaMongoException => throw new MongoException("Java driver: " + ex.getMessage, ex)
            case ex : BSONException => throw new MongoException("Java driver: " + ex.getMessage, ex)
        }
    }

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
            throw new MongoException("JavaDriver backend can't override query options on this operation")
            //result
        } else {
            body
        }
    }

    private def emptyQuery : DBObject = new BasicDBObject() // not immutable, so we always make a new one

    private def q[Q](query : Q)(implicit queryEncoder : QueryEncoder[Q]) =
        convertQueryToJava[Q](query)

    override def count[Q](query : Q, options : CountOptions)(implicit queryEncoder : QueryEncoder[Q]) : Long = withExceptionsMapped {
        withQueryFlags(options.overrideQueryFlags) {
            val fieldsQuery = if (options.fields.isDefined) options.fields.get : DBObject else emptyQuery
            if (options.limit.isDefined || options.skip.isDefined)
                collection.getCount(q(query), fieldsQuery, options.limit.getOrElse(0), options.skip.getOrElse(0))
            else
                collection.getCount(q(query), fieldsQuery)
        }
    }

    override def distinct[Q, V](key : String, options : DistinctOptions[Q])(implicit queryEncoder : QueryEncoder[Q], valueDecoder : ValueDecoder[V]) : Iterator[V] = withExceptionsMapped {
        import scala.collection.JavaConverters._

        val results : Seq[Any] = withQueryFlags(options.overrideQueryFlags) {
            if (options.query.isDefined)
                collection.distinct(key, q(options.query.get)).asScala.toSeq
            else
                collection.distinct(key).asScala.toSeq
        }

        results.map({ v => convertValueFromJava[V](v.asInstanceOf[AnyRef]) }).iterator
    }

    // adapter from DBCursor to our Cursor
    private class JavaCursor(cursor : DBCursor) extends Cursor[DBObject] {
        override def next() : DBObject = {
            cursor.next()
        }

        override def hasNext() : Boolean = {
            cursor.hasNext()
        }

        override def close() : Unit = {
            cursor.close()
        }
    }

    override def find[Q, E](query : Q, options : FindOptions)(implicit queryEncoder : QueryEncoder[Q], resultDecoder : QueryResultDecoder[E]) : Cursor[E] = withExceptionsMapped {
        import scala.collection.JavaConverters._

        val cursor = {
            if (options.fields.isDefined) {
                collection.find(q(query), options.fields.get)
            } else {
                collection.find(q(query))
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

        (new JavaCursor(cursor)).map(convertResultFromJava[E](_))
    }

    private def findOneImpl[E](query : DBObject, options : FindOneOptions)(implicit resultDecoder : QueryResultDecoder[E]) : Option[E] = {
        val results = withQueryFlags(options.overrideQueryFlags) {
            if (options.fields.isDefined) {
                Option(collection.findOne(query, options.fields.get))
            } else {
                Option(collection.findOne(query))
            }
        }
        results.map(convertResultFromJava[E](_))
    }

    override def findOne[Q, E](query : Q, options : FindOneOptions)(implicit queryEncoder : QueryEncoder[Q], resultDecoder : QueryResultDecoder[E]) : Option[E] = withExceptionsMapped {
        findOneImpl(q(query), options)
    }

    override def findOneById[I, E](id : I, options : FindOneByIdOptions)(implicit idEncoder : IdEncoder[I], resultDecoder : QueryResultDecoder[E]) : Option[E] = withExceptionsMapped {
        val query = new BasicDBObject()
        putIdToJava(query, "_id", id)
        findOneImpl(query, FindOneOptions(options.fields, options.overrideQueryFlags))
    }

    override def findIndexes() : Cursor[CollectionIndex] = withExceptionsMapped {
        // TODO
        throw new MongoException("findIndexes() not yet implemented")
    }

    override def findAndModify[Q, M, S, E](query : Q, modifier : Option[M], options : FindAndModifyOptions[S])(implicit queryEncoder : QueryEncoder[Q], modifierEncoder : ModifierEncoder[M], resultDecoder : QueryResultDecoder[E], sortEncoder : QueryEncoder[S]) : Option[E] = withExceptionsMapped {
        val result =
            if (options.flags.contains(FindAndModifyRemove)) {
                if (modifier.isDefined)
                    throw new IllegalArgumentException("Does not make sense to provide a replacement or modifier object to findAndModify with remove flag")
                Option(collection.findAndRemove(q(query)))
            } else if (!modifier.isDefined) {
                throw new IllegalArgumentException("Must provide a replacement or modifier object to findAndModify")
            } else if (options.flags.isEmpty && !options.fields.isDefined) {
                if (options.sort.isDefined)
                    Option(collection.findAndModify(q(query), q(options.sort.get),
                        convertModifierToJava(modifier.get)))
                else
                    Option(collection.findAndModify(q(query),
                        convertModifierToJava(modifier.get)))
            } else {
                Option(collection.findAndModify(q(query),
                    // getOrElse mixes poorly with implicit conversion from Fields
                    if (options.fields.isDefined) { options.fields.get : DBObject } else { emptyQuery },
                    options.sort.map(q(_)).getOrElse(emptyQuery),
                    false, // remove
                    convertModifierToJava(modifier.get),
                    options.flags.contains(FindAndModifyNew),
                    options.flags.contains(FindAndModifyUpsert)))
            }
        result.map(convertResultFromJava[E](_))
    }

    override def insert[E](o : E)(implicit upsertEncoder : UpsertEncoder[E]) : WriteResult = withExceptionsMapped {
        collection.insert(convertUpsertToJava(o))
    }

    override def save[Q](query : Q, options : UpdateOptions)(implicit queryEncoder : UpdateQueryEncoder[Q], upsertEncoder : UpsertEncoder[Q]) : WriteResult = withExceptionsMapped {
        collection.update(convertUpdateQueryToJava(query), convertUpsertToJava(query),
            options.flags.contains(UpdateUpsert), options.flags.contains(UpdateMulti))
    }

    override def update[Q, M](query : Q, modifier : M, options : UpdateOptions)(implicit queryEncoder : QueryEncoder[Q], modifierEncoder : ModifierEncoder[M]) : WriteResult = withExceptionsMapped {
        collection.update(q(query), convertModifierToJava(modifier),
            options.flags.contains(UpdateUpsert), options.flags.contains(UpdateMulti))
    }

    override def updateUpsert[Q, U](query : Q, update : U, options : UpdateOptions)(implicit queryEncoder : QueryEncoder[Q], upsertEncoder : UpsertEncoder[U]) : WriteResult = withExceptionsMapped {
        collection.update(q(query), convertUpsertToJava(update),
            options.flags.contains(UpdateUpsert), options.flags.contains(UpdateMulti))
    }

    override def remove[Q](query : Q)(implicit queryEncoder : QueryEncoder[Q]) : WriteResult = withExceptionsMapped {
        collection.remove(q(query))
    }

    override def removeById[I](id : I)(implicit idEncoder : IdEncoder[I]) : WriteResult = withExceptionsMapped {
        val query = new BasicDBObject()
        putIdToJava(query, "_id", id)
        collection.remove(query)
    }

    override def ensureIndex[Q](keys : Q, options : IndexOptions)(implicit queryEncoder : QueryEncoder[Q]) : WriteResult = withExceptionsMapped {
        val dbOptions = new BasicDBObject()
        options.name.foreach({ name =>
            dbOptions.put("name", name)
        })

        options.v foreach { v => dbOptions.put("v", v) }

        for (flag <- options.flags) {
            flag match {
                case IndexUnique => dbOptions.put("unique", true)
                case IndexBackground => dbOptions.put("background", true)
                case IndexDropDups => dbOptions.put("dropDups", true)
                case IndexSparse => dbOptions.put("sparse", true)
            }
        }

        collection.ensureIndex(q(keys), dbOptions)

        // Java driver returns void from ensureIndex (it just eats the writeResult)
        WriteResult(ok = true)
    }

    override def dropIndex(indexName : String) : CommandResult = withExceptionsMapped {
        collection.dropIndex(indexName)

        // Java driver returns void from dropIndex (it throws on failure)
        CommandResult(ok = true)
    }
}
