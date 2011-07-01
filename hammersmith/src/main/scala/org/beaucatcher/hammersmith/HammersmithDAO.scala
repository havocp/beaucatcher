package org.beaucatcher.hammersmith

import akka.actor.Channel
import akka.dispatch._
import org.beaucatcher.bson._
import org.beaucatcher.async._
import org.beaucatcher.mongo._
import com.mongodb.WriteResult
import com.mongodb.CommandResult
import com.mongodb.async.Collection
import com.mongodb.async.futures._
import org.bson.collection._
import org.bson._
import com.mongodb.async.Cursor
import com.mongodb.async.{ WriteResult => HammersmithWriteResult }
import akka.actor.Actor
import akka.actor.ActorRef
import org.bson.DefaultBSONSerializer
import org.bson.collection.Document
import java.util.concurrent.TimeUnit

private object CursorActor {
    // requests
    sealed trait Request
    case object Next extends Request

    // replies
    sealed trait Result
    case class Entry[T : SerializableBSONObject](doc : T) extends Result
    case object EOF extends Result
}

// actor used to convert a cursor into futures
private class CursorActor[T : SerializableBSONObject](val cursor : Cursor[T]) extends Actor {
    private def handleNext(sender : Channel[CursorActor.Result]) : Unit = {
        cursor.next() match {
            case Cursor.Entry(doc) =>
                sender ! CursorActor.Entry(doc.asInstanceOf[T])
            case Cursor.Empty =>
                cursor.nextBatch(() => { handleNext(sender) })
            case Cursor.EOF =>
                sender ! CursorActor.EOF
        }
    }

    def receive = {
        case CursorActor.Next =>
            // FIXME cast is no longer required when Akka upgrades (Channel now contravariant)
            handleNext(self.channel.asInstanceOf[Channel[CursorActor.Result]])
    }
}

// wrap a Cursor[T] in an Iterator[Future[T]]
private class CursorIterator[T : SerializableBSONObject](val cursor : Cursor[T]) extends Iterator[Future[T]] {
    private val actor = Actor.actorOf(new CursorActor(cursor)).start
    private var result : Option[Future[CursorActor.Result]] = None

    // ensure that result (current item) is not None.
    // when not None, it can be an Entry or EOF.
    private def check = {
        result match {
            case None =>
                result = Some(actor !!! CursorActor.Next)
            case Some(_) =>
        }
        if (!result.isDefined) {
            throw new IllegalStateException("result was None after check")
        }
    }

    override def hasNext = {
        check
        // time to block
        result.get.await.resultOrException.get match {
            case CursorActor.EOF =>
                actor.stop // clean up
                false
            case CursorActor.Entry(_) =>
                true
        }
    }

    override def next = {
        check
        val f = newPromise[T]
        result.get.onComplete({ completed : Future[CursorActor.Result] =>
            completed.result match {
                case None =>
                    f.completeWithException(new Exception("Timeout waiting on cursor"))
                case Some(CursorActor.EOF) =>
                    f.completeWithException(new NoSuchElementException("No more items in cursor"))
                case Some(CursorActor.Entry(doc)) =>
                    f.completeWithResult(doc.asInstanceOf[T])
            }
        })
        // need to get another item
        result = None
        // return the future for the previous item
        f
    }
}

private[hammersmith] class HammersmithAsyncDAO[EntityType : SerializableBSONObject, IdType <: AnyRef](protected val collection : Collection)
    extends AsyncDAO[BSONDocument, EntityType, IdType, Any] {
    private implicit def optionalfields2bsondocument(maybeFields : Option[Fields]) : BSONDocument = {
        maybeFields match {
            case Some(fields) =>
                val builder = Document.newBuilder
                for (i <- fields.included) {
                    builder += (i -> 1)
                }
                for (e <- fields.excluded) {
                    builder += (e -> 0)
                }
                builder.result
            case None =>
                Document.empty
        }
    }

    private def completeFromEither[T](f : DefaultCompletableFuture[T])(result : Either[Throwable, T]) : Unit = {
        result match {
            case Left(e) => f.completeWithException(e)
            case Right(o) => f.completeWithResult(o)
        }
    }

    private def translateWriteResult(h : HammersmithWriteResult) : WriteResult = {
        // FIXME
        null
    }

    private def completeWriteFromEither(f : DefaultCompletableFuture[WriteResult])(result : Either[Throwable, (Option[AnyRef], HammersmithWriteResult)]) : Unit = {
        result match {
            case Left(e) => f.completeWithException(e)
            case Right(valueAndResult) => f.completeWithResult(translateWriteResult(valueAndResult._2))
        }
    }

    // this is a workaround for a problem where we get a non-option but are wanting an option
    private def completeOptionalFromEither[T](f : DefaultCompletableFuture[Option[T]])(result : Either[Throwable, T]) : Unit = {
        result match {
            case Left(e) => f.completeWithException(e)
            case Right(o) => f.completeWithResult(Some(o))
        }
    }

    override def emptyQuery : BSONDocument =
        Document.empty

    override def count(query : BSONDocument, options : CountOptions) : Future[Long] = {
        val f = newPromise[Long]
        // FIXME handle options.overrideQueryFlags
        collection.count(query,
            options.fields : BSONDocument,
            options.limit.getOrElse(0),
            options.skip.getOrElse(0))(n => f.completeWithResult(n))
        f
    }

    override def distinct(key : String, options : DistinctOptions[BSONDocument]) : Future[Seq[Any]] = {
        val f = newPromise[Seq[Any]]
        // FIXME handle options.overrideQueryFlags
        collection.distinct(key, options.query.getOrElse(emptyQuery))({ seq => f.completeWithResult(seq) })
        f
    }

    override def find(query : BSONDocument, options : FindOptions) : Future[Iterator[Future[EntityType]]] = {
        // FIXME handle options.overrideQueryFlags
        val f = newPromise[Iterator[Future[EntityType]]]

        val rawBatchSize = options.batchSize.getOrElse(0)
        val rawLimit = options.limit.getOrElse(0L)
        val DEFAULT_BATCH = 100
        if (!rawLimit.isValidInt)
            throw new UnsupportedOperationException("Limits larger than Int.MaxValue aren't supported right now")
        val limit = if (rawLimit <= 0) Int.MaxValue else rawLimit
        val batchSize = math.min(if (rawBatchSize <= 0) DEFAULT_BATCH else rawBatchSize,
            limit)

        val handler = RequestFutures.query[EntityType]({ result : Either[Throwable, Cursor[EntityType]] =>
            result match {
                case Left(e) =>
                    f.completeWithException(e)
                case Right(c) =>
                    // items.take may ask for too many items in the last batch, could be
                    // fixed by making CursorIterator support a limit natively,
                    // but just an optimization
                    val items = new CursorIterator(c)
                    val limitedItems =
                        if (limit == Int.MaxValue)
                            items
                        else
                            items.take(limit.intValue)
                    f.completeWithResult(limitedItems)
            }
        })
        collection.find(query, options.fields : BSONDocument, options.skip.getOrElse(0L).intValue,
            batchSize.intValue)(handler)
        f
    }

    // FIXME shouldn't Hammersmith let us return None if query doesn't match ? or is that an exception ?
    override def findOne(query : BSONDocument, options : FindOneOptions) : Future[Option[EntityType]] = {
        val f = newPromise[Option[EntityType]]
        val handler = RequestFutures.findOne[EntityType](completeOptionalFromEither(f)(_))
        collection.findOne(query)(handler)
        f
    }

    // FIXME shouldn't Hammersmith let us return None if query doesn't match ? or is that an exception ?
    override def findOneById(id : IdType, options : FindOneByIdOptions) : Future[Option[EntityType]] = {
        val f = newPromise[Option[EntityType]]
        val handler = RequestFutures.findOne[EntityType](completeOptionalFromEither(f)(_))
        collection.findOneByID(id)(handler)
        f
    }

    override def entityToUpsertableObject(entity : EntityType) : BSONDocument = {
        entity match {
            case doc : BSONDocument =>
                doc
            case _ =>
                throw new UnsupportedOperationException("entityToUpsertableObject not implemented for " + entity)
        }
    }

    override def entityToModifierObject(entity : EntityType) : BSONDocument = {
        entity match {
            case doc : BSONDocument =>
                val builder = Document.newBuilder
                // strip _id
                doc.asMap foreach ({ kv => if (kv._1 != "_id") builder += kv })
                builder.result
            case _ =>
                throw new UnsupportedOperationException("entityToModifierObject not implemented for " + entity)
        }
    }

    override def entityToUpdateQuery(entity : EntityType) : BSONDocument = {
        entity match {
            case doc : BSONDocument =>
                val builder = Document.newBuilder
                // we want only _id
                builder += Pair("_id", doc.get("_id").get)
                builder.result
            case _ =>
                throw new UnsupportedOperationException("entityToUpdateQuery not implemented for " + entity)
        }
    }

    override def findAndModify(query : BSONDocument, update : Option[BSONDocument], options : FindAndModifyOptions[BSONDocument]) : Future[Option[EntityType]] = {
        val f = newPromise[Option[EntityType]]
        val handler = RequestFutures.command[EntityType](completeOptionalFromEither(f)(_))
        // query, sort, remove, update, getNew, fields, upsert
        collection.findAndModify(query,
            if (options.sort.isDefined) { options.sort.get : BSONDocument } else { emptyQuery },
            options.flags.contains(FindAndModifyRemove),
            update,
            options.flags.contains(FindAndModifyNew),
            if (options.fields.isDefined) { options.fields : BSONDocument } else { emptyQuery },
            options.flags.contains(FindAndModifyUpsert))(handler)
        f
    }

    override def insert(o : EntityType) : Future[WriteResult] = {
        val f = newPromise[WriteResult]
        val handler = RequestFutures.write(completeWriteFromEither(f)(_))
        collection.insert(o)(handler)
        f
    }

    override def update(query : BSONDocument, modifier : BSONDocument, options : UpdateOptions) : Future[WriteResult] = {
        val f = newPromise[WriteResult]
        val handler = RequestFutures.write(completeWriteFromEither(f)(_))
        collection.update(query, modifier, options.flags.contains(UpdateUpsert), options.flags.contains(UpdateMulti))(handler)
        f
    }

    override def remove(query : BSONDocument) : Future[WriteResult] = {
        val f = newPromise[WriteResult]
        val handler = RequestFutures.write(completeWriteFromEither(f)(_))
        collection.remove(query)(handler)
        f
    }
    override def removeById(id : IdType) : Future[WriteResult] = {
        val f = newPromise[WriteResult]
        val handler = RequestFutures.write(completeWriteFromEither(f)(_))
        val builder = Document.newBuilder
        builder += Pair("_id", id)
        collection.remove(builder.result, true)(handler)
        f
    }
}

private[hammersmith] class BObjectBSONDocument(bobject : BObject) extends BSONDocument {
    // we have to make a mutable map to make hammersmith happy, which pretty much blows
    override val self = scala.collection.mutable.HashMap.empty ++ bobject.unwrapped
    override def asMap = self
}

private[hammersmith] class BObjectHammersmithQueryComposer extends QueryComposer[BObject, BSONDocument] {
    override def queryIn(q : BObject) : BSONDocument = new BObjectBSONDocument(q)
    override def queryOut(q : BSONDocument) : BObject = BObject(q.toList map { kv => (kv._1, BValue.wrap(kv._2)) })
}

private[hammersmith] class BObjectHammersmithEntityComposer extends EntityComposer[BObject, BSONDocument] {
    override def entityIn(o : BObject) : BSONDocument = new BObjectBSONDocument(o)
    override def entityOut(o : BSONDocument) : BObject = BObject(o.toList map { kv => (kv._1, BValue.wrap(kv._2)) })
}

/**
 * A BObject DAO that specifically backends to a Hammersmith DAO.
 * Subclass would provide the backend and could override the in/out type converters.
 */
private[hammersmith] abstract trait BObjectHammersmithSyncDAO[OuterIdType, InnerIdType <: AnyRef]
    extends BObjectComposedSyncDAO[OuterIdType, BSONDocument, BSONDocument, InnerIdType, Any] {
    override protected val backend : SyncDAO[BSONDocument, BSONDocument, InnerIdType, Any]

    override protected val queryComposer : QueryComposer[BObject, BSONDocument]
    override protected val entityComposer : EntityComposer[BObject, BSONDocument]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
}

