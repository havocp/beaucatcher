package org.beaucatcher.async

import org.beaucatcher.bson._
import org.beaucatcher.mongo._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.routing._

private[async] sealed trait CollectionRequest
private[async] sealed trait CollectionReply {
    val result : Any
}

private[async] case class ErrorReply(override val result : Exception) extends CollectionReply

private[async] case class CountRequest[QueryType](query : QueryType, options : CountOptions) extends CollectionRequest
private[async] case class CountReply(override val result : Long) extends CollectionReply

private[async] case class DistinctRequest[QueryType](key : String, options : DistinctOptions[QueryType]) extends CollectionRequest
private[async] case class DistinctReply[ValueType](override val result : Seq[ValueType]) extends CollectionReply
private[async] case class FindRequest[QueryType](query : QueryType, options : FindOptions) extends CollectionRequest
private[async] case class FindReply[EntityType](override val result : Iterator[EntityType]) extends CollectionReply

private[async] case class OptionalEntityReply[EntityType](override val result : Option[EntityType]) extends CollectionReply
private[async] case class WriteReply(override val result : WriteResult) extends CollectionReply

private[async] case class FindOneRequest[QueryType](query : QueryType, options : FindOneOptions) extends CollectionRequest
private[async] case class FindOneByIdRequest[IdType](id : IdType, options : FindOneByIdOptions) extends CollectionRequest
private[async] case class FindAndModifyRequest[QueryType](query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) extends CollectionRequest

private[async] case class InsertRequest[EntityType](o : EntityType) extends CollectionRequest
private[async] case class UpdateRequest[QueryType](query : QueryType, modifier : QueryType, options : UpdateOptions) extends CollectionRequest
private[async] case class RemoveRequest[QueryType](query : QueryType) extends CollectionRequest
private[async] case class RemoveByIdRequest[IdType](id : IdType) extends CollectionRequest

private[async] case class CommandReply(override val result : CommandResult) extends CollectionReply

private[async] case class EnsureIndexRequest[QueryType](keys : QueryType, options : IndexOptions) extends CollectionRequest
private[async] case class DropIndexRequest(name : String) extends CollectionRequest
private[async] case object FindIndexesRequest extends CollectionRequest

// This could be implemented with typed actors, but not sure it's worth the dependency for now I guess
// This actor blocks on a synchronous Collection for each request. So, usually you want more than
// one of these in a pool.
// Hmm. FIXME we really want one pool globally, not per-Collection-type-parameterization, so this needs to
// just drop all its type parameters and pass a lot of Any around
private class BlockingCollectionActor[QueryType, EntityType, IdType, ValueType](val underlying : SyncCollection[QueryType, EntityType, IdType, ValueType])
    extends Actor {
    private def handleRequest(request : CollectionRequest) : CollectionReply = {
        // lots of casts in here, but don't know the right solution
        request match {
            case CountRequest(query, options) =>
                CountReply(underlying.count(query.asInstanceOf[QueryType], options))
            case DistinctRequest(key, options) =>
                DistinctReply(underlying.distinct(key, options.asInstanceOf[DistinctOptions[QueryType]]))
            case FindRequest(query, options) =>
                FindReply(underlying.find(query.asInstanceOf[QueryType], options))
            case FindOneRequest(query, options) =>
                OptionalEntityReply(underlying.findOne(query.asInstanceOf[QueryType], options))
            case FindOneByIdRequest(id, options) =>
                OptionalEntityReply(underlying.findOneById(id.asInstanceOf[IdType], options))
            case FindAndModifyRequest(query, update, options) =>
                OptionalEntityReply(underlying.findAndModify(query.asInstanceOf[QueryType],
                    update.asInstanceOf[Option[QueryType]],
                    options.asInstanceOf[FindAndModifyOptions[QueryType]]))
            case InsertRequest(o) =>
                WriteReply(underlying.insert(o.asInstanceOf[EntityType]))
            case UpdateRequest(query, modifier, options) =>
                WriteReply(underlying.update(query.asInstanceOf[QueryType], modifier.asInstanceOf[QueryType], options))
            case RemoveRequest(query) =>
                WriteReply(underlying.remove(query.asInstanceOf[QueryType]))
            case RemoveByIdRequest(id) =>
                WriteReply(underlying.removeById(id.asInstanceOf[IdType]))
            case EnsureIndexRequest(keys, options) =>
                CommandReply(underlying.ensureIndex(keys.asInstanceOf[QueryType], options))
            case DropIndexRequest(name) =>
                CommandReply(underlying.dropIndex(name))
            case FindIndexesRequest =>
                FindReply(underlying.findIndexes())
        }
    }

    override def receive = {
        case request : CollectionRequest =>
            val result = handleRequest(request)
            if (self.senderFuture.isDefined) {
                result match {
                    case ErrorReply(e) =>
                        self.senderFuture.get.completeWithException(e)
                    case success : CollectionReply =>
                        self.senderFuture.get.completeWithResult(success.result)
                }
            } else {
                self.sender.foreach(_ ! result)
            }
    }
}

/**
 * Actor pool; the goal is to have an actor per outstanding synchronous MongoDB request.
 * It's assumed that the underlying SyncCollection is thread-safe.
 * FIXME we really want one pool globally, not per-Collection-type-parameterization, so this needs to
 * just drop all its type parameters and pass a lot of Any around
 */
private[async] class CollectionActor[QueryType, EntityType, IdType, ValueType](val underlying : SyncCollection[QueryType, EntityType, IdType, ValueType])
    extends Actor
    with DefaultActorPool
    with BoundedCapacityStrategy
    with MailboxPressureCapacitor // overrides pressureThreshold based on mailboxes
    with SmallestMailboxSelector
    with Filter
    with RunningMeanBackoff
    with BasicRampup {

    override def receive = _route

    // BoundedCapacitor min and max actors in pool. No real rationale for the
    // upper bound here. Ideally it might be the number of connections in Casbah's
    // connection pool I suppose.
    override val lowerBound = 1
    override val upperBound = 32

    override val pressureThreshold = 1
    override val partialFill = true
    override val selectionCount = 1
    override val rampupRate = 0.1
    override val backoffRate = 0.50
    override val backoffThreshold = 0.50

    override def instance = Actor.actorOf(new BlockingCollectionActor[QueryType, EntityType, IdType, ValueType](underlying))
}
