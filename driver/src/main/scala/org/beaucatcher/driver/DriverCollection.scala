package org.beaucatcher.driver

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import org.beaucatcher.mongo._

private[beaucatcher] trait ReadOnlyDriverCollection {

    protected[beaucatcher] def context: DriverContext

    final def database: DriverDatabase = context.database

    def name: String

    // this has to be lazy since "database" may not be
    // valid yet during construct
    private lazy val _fullName = database.name + "." + name

    final def fullName: String = _fullName
}

/**
 * Common base class between SyncDriverCollection and AsyncDriverCollection,
 * holding only those operations that are always synchronous.
 */
private[beaucatcher] trait DriverCollection
    extends ReadOnlyDriverCollection {

}

private[beaucatcher] trait ReadOnlyAsyncDriverCollection
    extends ReadOnlyDriverCollection {

    /** Internal method to be sure we don't have more than 1 level of sync/async wrappers */
    private[beaucatcher] def underlyingSync: Option[ReadOnlySyncDriverCollection] =
        None

    def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[Long]

    def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Future[Iterator[V]]

    def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[AsyncCursor[E]]

    def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Future[Option[E]]

    def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Future[Option[E]]

    def findIndexes(): Future[AsyncCursor[CollectionIndex]]
}

private[beaucatcher] trait AsyncDriverCollection
    extends ReadOnlyAsyncDriverCollection
    with DriverCollection {

    private[beaucatcher] override def underlyingSync: Option[SyncDriverCollection] =
        None

    def findAndModify[Q, M, S, E](query: Q, modifier: Option[M], options: FindAndModifyOptions[S])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M], resultDecoder: QueryResultDecoder[E], sortEncoder: QueryEncoder[S]): Future[Option[E]]

    def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): Future[WriteResult]

    def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): Future[WriteResult]

    def updateUpsert[Q, U](query: Q, update: U, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[U]): Future[WriteResult]

    def save[Q](query: Q, options: UpdateOptions)(implicit queryEncoder: UpdateQueryEncoder[Q], upsertEncoder: UpsertEncoder[Q]): Future[WriteResult]

    def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult]

    def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): Future[WriteResult]

    def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): Future[WriteResult]

    def dropIndex(indexName: String): Future[CommandResult]
}

private[beaucatcher] object AsyncDriverCollection {
    def fromSync(sync: SyncDriverCollection)(implicit executor: ExecutionContext): AsyncDriverCollection = {
        sync.underlyingAsync.getOrElse({
            new AsyncCollectionWrappingSync(sync)
        })
    }
}

private[beaucatcher] trait ReadOnlySyncDriverCollection
    extends ReadOnlyDriverCollection {
    /** Internal method to be sure we don't have more than 1 level of sync/async wrappers */
    private[beaucatcher] def underlyingAsync: Option[ReadOnlyAsyncDriverCollection] =
        None

    def count[Q](query: Q, options: CountOptions)(implicit queryEncoder: QueryEncoder[Q]): Long

    def distinct[Q, V](key: String, options: DistinctOptions[Q])(implicit queryEncoder: QueryEncoder[Q], valueDecoder: ValueDecoder[V]): Iterator[V]

    def find[Q, E](query: Q, options: FindOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Cursor[E]

    def findOne[Q, E](query: Q, options: FindOneOptions)(implicit queryEncoder: QueryEncoder[Q], resultDecoder: QueryResultDecoder[E]): Option[E]

    def findOneById[I, E](id: I, options: FindOneByIdOptions)(implicit idEncoder: IdEncoder[I], resultDecoder: QueryResultDecoder[E]): Option[E]

    def findIndexes(): Cursor[CollectionIndex]
}

private[beaucatcher] trait SyncDriverCollection
    extends ReadOnlySyncDriverCollection
    with DriverCollection {
    private[beaucatcher] override def underlyingAsync: Option[AsyncDriverCollection] =
        None

    def findAndModify[Q, M, S, E](query: Q, modifier: Option[M], options: FindAndModifyOptions[S])(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M], resultDecoder: QueryResultDecoder[E], sortEncoder: QueryEncoder[S]): Option[E]

    def insert[E](o: E)(implicit upsertEncoder: UpsertEncoder[E]): WriteResult

    def update[Q, M](query: Q, modifier: M, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], modifierEncoder: ModifierEncoder[M]): WriteResult

    def updateUpsert[Q, U](query: Q, update: U, options: UpdateOptions)(implicit queryEncoder: QueryEncoder[Q], upsertEncoder: UpsertEncoder[U]): WriteResult

    def save[Q](query: Q, options: UpdateOptions)(implicit queryEncoder: UpdateQueryEncoder[Q], upsertEncoder: UpsertEncoder[Q]): WriteResult

    def remove[Q](query: Q)(implicit queryEncoder: QueryEncoder[Q]): WriteResult

    def removeById[I](id: I)(implicit idEncoder: IdEncoder[I]): WriteResult

    def ensureIndex[Q](keys: Q, options: IndexOptions)(implicit queryEncoder: QueryEncoder[Q]): WriteResult

    def dropIndex(indexName: String): CommandResult
}

private[beaucatcher] object SyncDriverCollection {
    def fromAsync(async: AsyncDriverCollection): SyncDriverCollection = {
        async.underlyingSync.getOrElse({
            new SyncCollectionWrappingAsync(async)
        })
    }
}