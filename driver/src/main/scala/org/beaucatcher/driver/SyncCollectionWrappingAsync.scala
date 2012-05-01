package org.beaucatcher.driver

import org.beaucatcher.mongo._
import akka.dispatch._
import akka.pattern._
import akka.util._
import akka.util.duration._

final private class SyncCollectionWrappingAsync(val underlying: AsyncDriverCollection)
    extends SyncDriverCollection {

    // FIXME make this configurable
    private val timeout = 31 seconds

    final private def block[T](future: Future[T]): T = {
        Await.result(future, timeout)
    }

    private[beaucatcher] final override def underlyingAsync =
        Some(underlying)

    final override def context =
        underlying.context

    final override def name =
        underlying.name

    final override def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Long =
        block(underlying.count(query, options))

    final override def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Iterator[V] =
        block(underlying.distinct(key, options))

    final override def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Cursor[E] =
        block(underlying.find(query, options)).blocking(timeout)

    final override def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E] =
        block(underlying.findOne(query, options))

    final override def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Option[E] =
        block(underlying.findOneById(id, options))

    final override def findIndexes(): Cursor[CollectionIndex] =
        block(underlying.findIndexes()).blocking(timeout)

    final override def findAndModify[Q, M, S, E](query: Q, modifier: Option[M], options: FindAndModifyOptions[S])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M], resultDecoder: QueryResultDecoder[E], sortEncoder: QueryEncoder[S]): Option[E] =
        block(underlying.findAndModify(query, modifier, options))

    final override def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): WriteResult =
        block(underlying.insert(o))

    final override def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): WriteResult =
        block(underlying.update(query, modifier, options))

    final override def updateUpsert[Q, U](query: Q, update: U, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[U]): WriteResult =
        block(underlying.updateUpsert(query, update, options))

    final override def save[Q](query: Q, options: UpdateOptions)(implicit queryEncoder: UpdateQueryEncoder[Q], upsertEncoder: UpsertEncoder[Q]): WriteResult =
        block(underlying.save(query, options))

    final override def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): WriteResult =
        block(underlying.remove(query))

    final override def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): WriteResult =
        block(underlying.removeById(id))

    final override def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): WriteResult =
        block(underlying.ensureIndex(keys, options))

    final override def dropIndex(indexName: String): CommandResult =
        block(underlying.dropIndex(indexName))
}
