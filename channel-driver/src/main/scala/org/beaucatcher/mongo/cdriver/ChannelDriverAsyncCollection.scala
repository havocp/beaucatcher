package org.beaucatcher.mongo.cdriver

import akka.dispatch._
import akka.util.duration._
import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.channel._
import org.beaucatcher.wire._

private[cdriver] abstract trait ChannelDriverAsyncCollection[QueryType, EntityType, IdType <: Any, ValueType] extends AsyncCollection[QueryType, EntityType, IdType, ValueType] {

    import RawEncoded._

    private[beaucatcher] override def context: ChannelDriverContext

    private[cdriver] def connection: Connection = context.connection

    override def name: String

    protected[beaucatcher] implicit def queryEncoder: QueryEncoder[QueryType]
    protected[beaucatcher] implicit def entityDecoder: QueryResultDecoder[EntityType]
    protected[beaucatcher] implicit def entityEncoder: EntityEncodeSupport[EntityType]

    private def newRaw() = RawEncoded(context.driver.backend)
    // must be lazy since it uses stuff that isn't available at construct
    private implicit lazy val fieldsEncoder = newFieldsQueryEncoder(context.driver.backend)

    override def count(query: QueryType, options: CountOptions): Future[Long] = {
        val raw = newRaw()
        raw.writeField("count", name)
        raw.writeField("query", query)
        raw.writeFieldLongOption("limit", options.limit)
        raw.writeFieldLongOption("skip", options.skip)
        raw.writeField("fields", options.fields)

        connection.sendCommand(queryFlags(options.overrideQueryFlags), database.name, raw) map { reply =>
            val result = decodeCommandResult(reply, "n")
            result.result.throwIfNotOk()
            result.fields.get("n") match {
                case Some(n: Number) =>
                    n.longValue()
                case _ =>
                    throw new MongoException("Missing/incorrect 'n' field in count command result: " + result)
            }
        }
    }

    protected def wrapValue(v: Any): ValueType

    // TODO there is no reason this should have an Iterator[Future] because it
    // never does batch paging like a cursor it looks like
    override def distinct(key: String, options: DistinctOptions[QueryType]): Future[Iterator[Future[ValueType]]] = {
        val raw = newRaw()
        raw.writeField("distinct", name)
        raw.writeField("key", key)
        raw.writeField("query", options.query)

        connection.sendCommand(queryFlags(options.overrideQueryFlags), database.name, raw) map { reply =>
            val result = decodeCommandResult(reply, "values")
            result.result.throwIfNotOk()
            result.fields.get("values") match {
                case Some(s: Seq[_]) =>
                    s.map({ v =>
                        Promise.successful(wrapValue(v))(connection.system.dispatcher)
                    }).iterator
                case _ =>
                    throw new MongoException("Missing/incorrect 'values' field in distinct command result: " + result)
            }
        }
    }

    private def computeBatchSize(rawBatchSize: Int, rawLimit: Long): (Int, Long) = {
        val DEFAULT_BATCH = 100
        val limit = if (rawLimit <= 0) Long.MaxValue else rawLimit
        val batchSize = math.min(if (rawBatchSize <= 0) DEFAULT_BATCH else rawBatchSize,
            math.min(limit, Int.MaxValue).intValue())
        (batchSize, limit)
    }

    // TODO we want to change the interface to use AsyncCursor natively so this
    // conversion should go away. This function is a temporary hack.
    private def cursorToIterator(cursor: AsyncCursor[EntityType]): Iterator[Future[EntityType]] = {
        // blocking is the simplest, so use that for our temp hack.
        cursor.blocking(1 minute).map(Promise.successful(_)(connection.system.dispatcher))
    }

    override def find(query: QueryType, options: FindOptions): Future[Iterator[Future[EntityType]]] = {
        val skip = options.skip.getOrElse(0L)
        val (batchSize, limit) = computeBatchSize(options.batchSize.getOrElse(0), options.limit.getOrElse(0L))

        connection.sendQuery[QueryType, Fields](queryFlags(options.overrideQueryFlags), fullName,
            skip.intValue, batchSize, limit, query, options.fields)
            .map({ replyCursor =>
                replyCursor.flatMap({ reply =>
                    reply.iterator[EntityType]()
                })
            })
            .map({ entityCursor =>
                cursorToIterator(entityCursor)
            })
    }

    private def findOne[Q](flags: Int, query: Q, fields: Option[Fields])(implicit querySupport: QueryEncoder[Q]): Future[Option[EntityType]] = {
        connection.sendQueryOne[Q, Fields](flags, fullName, query, fields)
            .map({ replyOption =>
                replyOption.map({ reply =>
                    reply.iterator[EntityType]().next()
                })
            })
    }

    override def findOne(query: QueryType, options: FindOneOptions): Future[Option[EntityType]] = {
        findOne(queryFlags(options.overrideQueryFlags), query, options.fields)
    }

    override def findOneById(id: IdType, options: FindOneByIdOptions): Future[Option[EntityType]] = {
        val raw = newRaw()
        raw.writeFieldAny("_id", id)
        findOne(queryFlags(options.overrideQueryFlags), raw, options.fields)
    }

    override def findIndexes(): Future[Iterator[Future[CollectionIndex]]] =
        throw new UnsupportedOperationException("FIXME") // TODO

    override def entityToUpsertableObject(entity: EntityType): QueryType

    override def entityToModifierObject(entity: EntityType): QueryType

    override def entityToUpdateQuery(entity: EntityType): QueryType

    override def findAndModify(query: QueryType, update: Option[QueryType], options: FindAndModifyOptions[QueryType]): Future[Option[EntityType]] = {
        val raw = newRaw()
        raw.writeField("findandmodify", name)
        raw.writeField("query", query)
        raw.writeField("fields", options.fields)
        raw.writeField("sort", options.sort)
        raw.writeField("update", update)
        for (flag <- options.flags) {
            flag match {
                case FindAndModifyNew =>
                    raw.writeField("new", true)
                case FindAndModifyRemove =>
                    raw.writeField("remove", true)
                case findAndModifyUpsert =>
                    raw.writeField("upsert", true)
            }
        }

        connection.sendCommand(0 /* flags */ , database.name, raw) map { reply =>
            // TODO the Java driver has a hack to handle an error with string
            // "No matching object found" ... unclear if modern mongo needs
            // the hack, but at least test the case.
            val result = decodeCommandResult[EntityType](reply, "value")
            if (result.result.ok) {
                result.fields.get("value") match {
                    case Some(null) =>
                        None
                    case Some(x) =>
                        // note: this asInstanceOf doesn't do anything other than
                        // make it compile, due to erasure
                        Some(x.asInstanceOf[EntityType])
                    case _ =>
                        None
                }
            } else if (result.result.errmsg == "No matching object found") {
                // This case is in the Java driver, don't know which versions
                // of mongo require it or what
                None
            } else {
                result.result.throwIfNotOk()
                None // not reached
            }
        }
    }

    // this has to be lazy or else "database" won't be ready
    private lazy val safe = GetLastError.safe(database.name)

    private def throwOnFail(f: Future[WriteResult]): Future[WriteResult] = {
        f.map(_.throwIfNotOk())
    }

    override def insert(o: EntityType): Future[WriteResult] = {
        // TODO have a way to set "continue on error" flag, which is in the
        // WriteConcern in the Java driver.
        throwOnFail(connection.sendInsert(0 /* flags */ , fullName, Seq(o), safe))
    }

    override def update(query: QueryType, modifier: QueryType, options: UpdateOptions): Future[WriteResult] = {
        var flags = 0
        for (flag <- options.flags) {
            flag match {
                case UpdateUpsert =>
                    flags |= Mongo.UPDATE_FLAG_UPSERT
                case UpdateMulti =>
                    flags |= Mongo.UPDATE_FLAG_MULTI_UPDATE
            }
        }
        throwOnFail(connection.sendUpdate(fullName, flags, query, modifier, safe))
    }

    private def remove[Q](flags: Int, query: Q)(implicit querySupport: QueryEncoder[Q]): Future[WriteResult] = {
        throwOnFail(connection.sendDelete(fullName, flags, query, safe))
    }

    override def remove(query: QueryType): Future[WriteResult] = {
        // TODO support DELETE_FLAG_SINGLE_REMOVE (Java driver just sets
        // this iff the query has an _id in it)
        remove[QueryType](0 /* flags */ , query)
    }

    override def removeById(id: IdType): Future[WriteResult] = {
        val raw = newRaw()
        raw.writeFieldAny("_id", id)
        remove[RawEncoded](Mongo.DELETE_FLAG_SINGLE_REMOVE, raw)
    }

    // TODO don't do it this way?
    private def indexNameHack[Q](keys: Q)(implicit querySupport: QueryEncoder[Q]): String = {
        import Codecs._
        // convert to BObject via serializing
        val buf = context.driver.backend.newDynamicEncodeBuffer(64)
        querySupport.encode(buf, keys)
        val bobj = implicitly[QueryResultDecoder[BObject]].decode(buf.toDecodeBuffer())
        // then compute the index name
        defaultIndexName(bobj)
    }

    override def ensureIndex(keys: QueryType, options: IndexOptions): Future[WriteResult] = {
        val indexName = options.name.getOrElse(indexNameHack(keys))
        val raw = newRaw()
        raw.writeField("name", indexName)
        raw.writeField("ns", fullName)
        raw.writeFieldIntOption("v", options.v)
        for (flag <- options.flags) {
            flag match {
                case IndexUnique => raw.writeField("unique", true)
                case IndexBackground => raw.writeField("background", true)
                case IndexDropDups => raw.writeField("dropDups", true)
                case IndexSparse => raw.writeField("sparse", true)
            }
        }
        raw.writeField[QueryType]("key", keys)

        connection.sendInsert(0 /* flags */ , database.name + ".system.indexes", Seq(raw), safe)
    }

    override def dropIndex(indexName: String): Future[CommandResult] = {
        val raw = newRaw()
        raw.writeField("deleteIndexes", name)
        raw.writeField("index", indexName)
        connection.sendCommand(0 /* query flags */ , database.name, raw) map { reply =>
            decodeCommandResult(reply).result.throwIfNotOk()
        }
    }
}

// This class is a temporary hack in essence; we want to remove
// BObject knowledge from the driver.
private[cdriver] abstract class BObjectChannelDriverAsyncCollection[IdType]
    extends ChannelDriverAsyncCollection[BObject, BObject, IdType, BValue]
    with BObjectAsyncCollection[IdType] {

    protected[beaucatcher] override implicit def queryEncoder: QueryEncoder[BObject] = Codecs.bobjectQueryEncoder
    protected[beaucatcher] override implicit def entityDecoder: QueryResultDecoder[BObject] = Codecs.bobjectQueryResultDecoder
    protected[beaucatcher] override implicit def entityEncoder: EntityEncodeSupport[BObject] = Codecs.bobjectEntityEncodeSupport

    protected override def wrapValue(v: Any): BValue = {
        BValue.wrap(v)
    }

    override def emptyQuery = BObject.empty

    override def entityToUpsertableObject(entity: BObject): BObject = {
        entity
    }

    override def entityToModifierObject(entity: BObject): BObject = {
        // not allowed to change the _id
        entity - "_id"
    }

    override def entityToUpdateQuery(entity: BObject): BObject = {
        BObject("_id" -> entity.getOrElse("_id", throw new IllegalArgumentException("only objects with an _id field work here")))
    }
}

// Another temporary hack because right now drivers know about particular
// query/entity types
private[cdriver] abstract class EntityChannelDriverAsyncCollection[EntityType <: AnyRef, IdType](val entityBObjectEntityComposer: EntityComposer[EntityType, BObject])(
    override implicit val entityDecoder: QueryResultDecoder[EntityType],
    override implicit val entityEncoder: EntityEncodeSupport[EntityType])
    extends ChannelDriverAsyncCollection[BObject, EntityType, IdType, Any]
    with EntityAsyncCollection[BObject, EntityType, IdType] {
    override implicit def queryEncoder: QueryEncoder[BObject] = Codecs.bobjectQueryEncoder

    protected override def wrapValue(v: Any): Any = {
        v
    }

    override def emptyQuery = BObject.empty

    override def entityToUpsertableObject(entity: EntityType): BObject = {
        entityBObjectEntityComposer.entityIn(entity)
    }

    override def entityToModifierObject(entity: EntityType): BObject = {
        // not allowed to change the _id
        entityBObjectEntityComposer.entityIn(entity) - "_id"
    }

    override def entityToUpdateQuery(entity: EntityType): BObject = {
        val bobj = entityBObjectEntityComposer.entityIn(entity)
        BObject("_id" -> bobj.getOrElse("_id", throw new IllegalArgumentException("only objects with an _id field work here")))
    }
}
