package org.beaucatcher.driver

import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.dispatch._

final private class AsyncCollectionWrappingSync(val underlying: SyncDriverCollection)(implicit val executionContext: ExecutionContext)
    extends AsyncDriverCollection {

    private[beaucatcher] final override def underlyingSync =
        Some(underlying)

    final override def context =
        underlying.context
    final override def name =
        underlying.name

    def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[Long] =
        Future({ Future.blocking(); underlying.count(query, options) })

    def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Future[Iterator[V]] =
        Future({ Future.blocking(); underlying.distinct(key, options) })

    def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]] =
        Future({ Future.blocking(); AsyncCursor(underlying.find(query, options)) })

    def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        Future({ Future.blocking(); underlying.findOne(query, options) })

    def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Future[Option[E]] =
        Future({ Future.blocking(); underlying.findOneById(id, options) })

    def findIndexes(): Future[AsyncCursor[CollectionIndex]] =
        Future({ Future.blocking(); AsyncCursor(underlying.findIndexes()) })

    def findAndModify[Q, M, S, E](query: Q, modifier: Option[M], options: FindAndModifyOptions[S])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M], resultDecoder: QueryResultDecoder[E], sortEncoder: QueryEncoder[S]): Future[Option[E]] =
        Future({ Future.blocking(); underlying.findAndModify(query, modifier, options) })

    def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.insert(o) })

    def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.update(query, modifier, options) })

    def updateUpsert[Q, U](query: Q, update: U, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[U]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.updateUpsert(query, update, options) })

    def save[Q](query: Q, options: UpdateOptions)(implicit queryEncoder: UpdateQueryEncoder[Q], upsertEncoder: UpsertEncoder[Q]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.save(query, options) })

    def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.remove(query) })

    def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.removeById(id) })

    def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult] =
        Future({ Future.blocking(); underlying.ensureIndex(keys, options) })

    def dropIndex(indexName: String): Future[CommandResult] =
        Future({ Future.blocking(); underlying.dropIndex(indexName) })
}
