package org.beaucatcher.hammersmith

import akka.dispatch.Future
import akka.dispatch.DefaultCompletableFuture
import org.beaucatcher.bson._
import org.beaucatcher.async._
import org.beaucatcher.mongo._
import com.mongodb.WriteResult
import com.mongodb.CommandResult
import com.mongodb.async.Collection
import com.mongodb.async.futures._
import org.bson.collection.BSONDocument
import com.mongodb.async.Cursor
import akka.actor.Actor
import akka.actor.ActorRef
import org.bson.DefaultBSONSerializer

private object CursorActor {
    // requests
    sealed trait Request
    case object Next extends Request

    // replies
    sealed trait Result
    case class Entry(doc : BSONDocument) extends Result
    case object EOF extends Result
}

private class CursorActor(val cursor : Cursor) extends Actor {
    private def handleNext(sender : ActorRef) : Unit = {
        cursor.next() match {
            case Cursor.Entry(doc) =>
                sender ! CursorActor.Entry(doc)
            case Cursor.Empty =>
                cursor.nextBatch(() => { handleNext(sender) })
            case Cursor.EOF =>
                sender ! CursorActor.EOF
        }
    }

    def receive = {
        case CursorActor.Next => handleNext(self.sender.get)
    }
}

private class CursorIterator(val cursor : Cursor) extends Iterator[Future[AsyncDAO.CursorResult[BSONDocument]]] {
    private val actor = Actor.actorOf(new CursorActor(cursor)).start
    private var result : Option[Future[CursorActor.Result]] = None

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
            case CursorActor.EOF => false
            case CursorActor.Entry(_) => true
        }
    }

    override def next = {
        check
        val f = new DefaultCompletableFuture[AsyncDAO.CursorResult[BSONDocument]]
        result.get.onComplete({ completed : Future[CursorActor.Result] =>
            completed.result match {
                case None => f.completeWithException(new Exception("Timeout waiting on cursor"))
                case Some(CursorActor.EOF) => f.completeWithResult(AsyncDAO.EOF)
                case Some(CursorActor.Entry(doc)) => f.completeWithResult(AsyncDAO.Entry(doc))
            }
        })
        f
    }
}

abstract trait HammersmithAsyncDAO[IdType <: AnyRef] extends AsyncDAO[BSONDocument, BSONDocument, IdType] {
    protected def collection : Collection

    private def completeFromEither[T](f : DefaultCompletableFuture[T])(result : Either[Throwable, T]) : Unit = {
        result match {
            case Left(e) => f.completeWithException(e)
            case Right(o) => f.completeWithResult(o)
        }
    }

    // this is a workaround for a problem where we get a non-option but are wanting an option
    private def completeOptionalFromEither[T](f : DefaultCompletableFuture[Option[T]])(result : Either[Throwable, T]) : Unit = {
        result match {
            case Left(e) => f.completeWithException(e)
            case Right(o) => f.completeWithResult(Some(o))
        }
    }

    override def find[A <% BSONDocument](ref : A) : Future[Iterator[Future[AsyncDAO.CursorResult[BSONDocument]]]] = {
        val f = new DefaultCompletableFuture[Iterator[Future[AsyncDAO.CursorResult[BSONDocument]]]]
        val handler = RequestFutures.query[Cursor]({ result =>
            result match {
                case Left(e) => f.completeWithException(e)
                case Right(c) => f.completeWithResult(new CursorIterator(c))
            }
        })
        collection.find(ref)(handler)
        f
    }

    // FIXME shouldn't Hammersmith let us return None if query doesn't match ? or is that an exception ? 
    override def findOne[A <% BSONDocument](t : A) : Future[Option[BSONDocument]] = {
        val f = new DefaultCompletableFuture[Option[BSONDocument]]
        val handler = RequestFutures.findOne[BSONDocument](completeOptionalFromEither(f)(_))
        collection.findOne(t)(handler)
        f
    }

    override def findOneByID(id : IdType) : Future[Option[BSONDocument]] = {
        val f = new DefaultCompletableFuture[Option[BSONDocument]]
        val handler = RequestFutures.findOne[BSONDocument](completeOptionalFromEither(f)(_))
        collection.findOneByID(id)(handler)
        f
    }

    override def findAndModify[A <% BSONDocument](q : A, t : BSONDocument) : Future[Option[BSONDocument]] = {
        val f = new DefaultCompletableFuture[Option[BSONDocument]]
        val handler = RequestFutures.findOne[BSONDocument](completeOptionalFromEither(f)(_))
        // FIXME not in hammersmith yet
        //collection.findAndModify(q, t)(handler)
        f
    }

    override def save(t : BSONDocument) : Future[WriteResult] = {
        // FIXME
        val f = new DefaultCompletableFuture[WriteResult]
        f
    }

    override def insert(t : BSONDocument) : Future[WriteResult] = {
        // FIXME
        val f = new DefaultCompletableFuture[WriteResult]
        f
    }

    override def update[A <% BSONDocument](q : A, o : BSONDocument) : Future[WriteResult] = {
        // FIXME
        val f = new DefaultCompletableFuture[WriteResult]
        f
    }

    override def remove(t : BSONDocument) : Future[WriteResult] = {
        // FIXME
        val f = new DefaultCompletableFuture[WriteResult]
        f
    }
}

/**
 * Base trait that chains SyncDAO methods to a Hammersmith collection, which must be provided
 * by a subclass of this trait.
 */
abstract trait HammersmithSyncDAO[IdType <: AnyRef] extends SyncDAO[BSONDocument, BSONDocument, IdType] {
    protected def collection : Collection

    private class ConcreteAsyncDAO(override val collection : Collection) extends HammersmithAsyncDAO[IdType]

    private lazy val async = new ConcreteAsyncDAO(collection)

    override def find[A <% BSONDocument](ref : A) : Iterator[BSONDocument] = {
        val futuresIterator = async.find(ref).await.resultOrException.get
        futuresIterator flatMap {
            _.await.resultOrException.get match {
                case AsyncDAO.Entry(doc) => Some(doc)
                case AsyncDAO.EOF => None
            }
        }
    }

    override def findOne[A <% BSONDocument](t : A) : Option[BSONDocument] = {
        async.findOne(t).await.resultOrException.get
    }

    override def findOneByID(id : IdType) : Option[BSONDocument] = {
        async.findOneByID(id).await.resultOrException.get
    }

    override def findAndModify[A <% BSONDocument](q : A, t : BSONDocument) : Option[BSONDocument] = {
        async.findAndModify(q, t).await.resultOrException.get
    }

    override def save(t : BSONDocument) : WriteResult = {
        async.save(t).await.resultOrException.get
    }

    override def insert(t : BSONDocument) : WriteResult = {
        async.insert(t).await.resultOrException.get
    }

    override def update[A <% BSONDocument](q : A, o : BSONDocument) : WriteResult = {
        async.update(q, o).await.resultOrException.get
    }

    override def remove(t : BSONDocument) : WriteResult = {
        async.remove(t).await.resultOrException.get
    }
}

private[hammersmith] class BObjectBSONDocument(bobject : BObject) extends BSONDocument {
    // we have to make a mutable map to make hammersmith happy, which pretty much blows
    override val self = scala.collection.mutable.HashMap.empty ++ bobject.unwrapped
    override def asMap = self
    override val serializer = new DefaultBSONSerializer
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
    extends BObjectComposedSyncDAO[OuterIdType, BSONDocument, BSONDocument, InnerIdType] {
    override protected val backend : HammersmithSyncDAO[InnerIdType]

    override protected val queryComposer : QueryComposer[BObject, BSONDocument]
    override protected val entityComposer : EntityComposer[BObject, BSONDocument]
    override protected val idComposer : IdComposer[OuterIdType, InnerIdType]
}

