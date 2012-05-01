package org.beaucatcher.mongo.cdriver

import akka.dispatch._
import akka.util.duration._
import org.beaucatcher.mongo._
import org.beaucatcher.bson._
import org.beaucatcher.channel._
import org.beaucatcher.driver._
import org.beaucatcher.wire._

private[cdriver] final class ChannelDriverAsyncCollection(override val name: String, override val context: ChannelDriverContext) extends AsyncDriverCollection {

    import RawEncoded._

    private[cdriver] def connection: Connection = context.connection

    private def newRaw() = RawEncoded(context.driver.backend)
    // must be lazy since it uses stuff that isn't available at construct
    private implicit lazy val fieldsEncoder = newFieldsQueryEncoder(context.driver.backend)

    override def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[Long] = {
        val raw = newRaw()
        raw.writeField("count", name)
        raw.writeField("query", query)
        raw.writeFieldLongOption("limit", options.limit)
        raw.writeFieldLongOption("skip", options.skip)
        raw.writeField("fields", options.fields)

        connection.sendCommand(queryFlags(options.overrideQueryFlags), database.name, raw) map { reply =>
            val result = decodeCommandResult[BugIfDecoded](reply, "n")
            result.result.throwIfNotOk()
            result.fields.get("n") match {
                case Some(n: Number) =>
                    n.longValue()
                case _ =>
                    throw new MongoException("Missing/incorrect 'n' field in count command result: " + result)
            }
        }
    }

    // TODO there is no reason this should have an Iterator[Future] because it
    // never does batch paging like a cursor it looks like
    override def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Future[Iterator[V]] = {
        import Codecs._

        val raw = newRaw()
        raw.writeField("distinct", name)
        raw.writeField("key", key)
        raw.writeField("query", options.query)

        connection.sendCommand(queryFlags(options.overrideQueryFlags), database.name, raw) map { reply =>
            val result = decodeCommandResultFields[BugIfDecoded](reply, RawField("values", Some(RawBufferDecoded.rawBufferValueDecoder)))
            result.result.throwIfNotOk()
            result.fields.get("values") match {
                case Some(RawBufferDecoded(buf)) =>
                    // 'buf' should be an encoded array value
                    readArrayValues[V](buf).iterator
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

    override def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] = {
        val skip = options.skip.getOrElse(0L)
        val (batchSize, limit) = computeBatchSize(options.batchSize.getOrElse(0), options.limit.getOrElse(0L))

        connection.sendQuery[Q, Fields](queryFlags(options.overrideQueryFlags), fullName,
            skip.intValue, batchSize, limit, query, options.fields)
            .map({ replyCursor =>
                replyCursor.flatMap({ reply =>
                    reply.iterator[E]()
                })
            })
    }

    private def findOne[Q, E](flags: Int, query: Q, fields: Option[Fields])(implicit querySupport: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] = {
        connection.sendQueryOne[Q, Fields](flags, fullName, query, fields)
            .map({ replyOption =>
                replyOption.map({ reply =>
                    reply.iterator[E]().next()
                })
            })
    }

    override def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] = {
        findOne(queryFlags(options.overrideQueryFlags), query, options.fields)
    }

    override def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] = {
        val raw = newRaw()
        raw.writeFieldAny("_id", id)
        findOne(queryFlags(options.overrideQueryFlags), raw, options.fields)
    }

    override def findIndexes(): Future[AsyncCursor[CollectionIndex]] =
        throw new UnsupportedOperationException("FIXME") // TODO

    override def findAndModify[Q, M, S, E](query: Q, modifier: Option[M], options: FindAndModifyOptions[S])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M], resultDecoder: QueryResultDecoder[E], sortEncoder: QueryEncoder[S]): Future[Option[E]] = {
        if (options.flags.contains(FindAndModifyRemove)) {
            if (modifier.isDefined) {
                throw new BugInSomethingMongoException("Does not make sense to provide a replacement or modifier object to findAndModify with remove flag")
            }
        } else if (modifier.isEmpty) {
            throw new BugInSomethingMongoException("Must provide a replacement or modifier object to findAndModify")
        }

        val raw = newRaw()
        raw.writeField("findandmodify", name)
        raw.writeField("query", query)
        raw.writeField("fields", options.fields)
        raw.writeField("sort", options.sort)
        raw.writeFieldAsModifier("update", modifier)
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
            val result = decodeCommandResult[E](reply, "value")
            if (result.result.ok) {
                result.fields.get("value") match {
                    case Some(null) =>
                        None
                    case Some(x) =>
                        // note: this asInstanceOf doesn't do anything other than
                        // make it compile, due to erasure
                        Some(x.asInstanceOf[E])
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

    override def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): Future[WriteResult] = {
        // TODO have a way to set "continue on error" flag, which is in the
        // WriteConcern in the Java driver.
        throwOnFail(connection.sendInsert(0 /* flags */ , fullName, Seq(o), safe))
    }

    private def updateFlags(options: UpdateOptions): Int = {
        var flags = 0
        for (flag <- options.flags) {
            flag match {
                case UpdateUpsert =>
                    flags |= Mongo.UPDATE_FLAG_UPSERT
                case UpdateMulti =>
                    flags |= Mongo.UPDATE_FLAG_MULTI_UPDATE
            }
        }
        flags
    }

    override def save[Q](query: Q, options: UpdateOptions)(implicit queryEncoder: UpdateQueryEncoder[Q], upsertEncoder: UpsertEncoder[Q]): Future[WriteResult] = {
        throwOnFail(connection.sendSave(fullName, updateFlags(options), query, safe))
    }

    override def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): Future[WriteResult] = {
        throwOnFail(connection.sendUpdate(fullName, updateFlags(options), query, modifier, safe))
    }

    override def updateUpsert[Q, U](query: Q, modifier: U, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[U]): Future[WriteResult] = {
        throwOnFail(connection.sendUpdateUpsert(fullName, updateFlags(options), query, modifier, safe))
    }

    private def remove[Q](flags: Int, query: Q)(implicit querySupport: QueryEncoder[Q]): Future[WriteResult] = {
        throwOnFail(connection.sendDelete(fullName, flags, query, safe))
    }

    override def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] = {
        // TODO support DELETE_FLAG_SINGLE_REMOVE (Java driver just sets
        // this iff the query has an _id in it)
        remove[Q](0 /* flags */ , query)
    }

    override def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): Future[WriteResult] = {
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

    override def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] = {
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
        raw.writeField[Q]("key", keys)

        connection.sendInsert(0 /* flags */ , database.name + ".system.indexes", Seq(raw), safe)
    }

    override def dropIndex(indexName: String): Future[CommandResult] = {
        val raw = newRaw()
        raw.writeField("deleteIndexes", name)
        raw.writeField("index", indexName)
        connection.sendCommand(0 /* query flags */ , database.name, raw) map { reply =>
            decodeCommandResult[BugIfDecoded](reply).result.throwIfNotOk()
        }
    }
}
