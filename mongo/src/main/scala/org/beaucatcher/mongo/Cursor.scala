package org.beaucatcher.mongo

import akka.dispatch._
import akka.util._
import akka.pattern._
import scala.collection.Iterator
import java.io.Closeable
import scala.annotation.tailrec
import scala.collection.GenTraversableOnce
import java.util.concurrent.ConcurrentLinkedQueue

trait Cursor[+T] extends Iterator[T] with Closeable {
    def close() : Unit
}

trait Batch[+T] extends Traversable[T] {
    self =>

    /** Is this the isLastBatch batch? */
    def isLastBatch : Boolean

    protected[this] def getMore() : Future[Batch[T]]

    private lazy val _more = getMore()

    /** If not isLastBatch, we can fetch the subsequent one */
    final def anotherBatch() : Future[Batch[T]] = if (isLastBatch) {
        Promise.failed(new MongoException("No more batches, cursor exhausted"))
    } else {
        _more
    }

    protected[beaucatcher] implicit def executor : ExecutionContext

    // this has to make a new iterator each time
    protected[beaucatcher] def iterator() : Iterator[T]

    final override def foreach[U](f : T => U) : Unit = iterator().foreach(f)
    final override def isEmpty : Boolean = iterator().isEmpty
    final override def hasDefiniteSize : Boolean = true
    final override def copyToArray[B >: T](xs : Array[B], start : Int, len : Int) = iterator.copyToArray(xs, start, len)
    final override def find(p : T => Boolean) : Option[T] = iterator().find(p)
    final override def exists(p : T => Boolean) : Boolean = iterator().exists(p)
    final override def forall(p : T => Boolean) : Boolean = iterator().forall(p)
    final override def seq : Batch[T] = self
    final override def toTraversable : Traversable[T] = iterator().toTraversable
    final override def toIterator : Iterator[T] = iterator()
    final override def toStream : Stream[T] = iterator().toStream

    final def map[B](f : T => B) : Batch[B] = new Batch[B]() {
        override implicit def executor = self.executor
        override def isLastBatch = self.isLastBatch
        override def getMore() = self.anotherBatch().map(_.map(f))
        override def iterator() = self.iterator().map(f)
    }

    final def flatMap[B](f : T => GenTraversableOnce[B]) : Batch[B] = new Batch[B]() {
        override implicit def executor = self.executor
        override def isLastBatch = self.isLastBatch
        override def getMore() = self.anotherBatch().map(_.flatMap(f))
        override def iterator() = self.iterator().flatMap(f)
    }

    /** When getMore() gets to the isLastBatch batch, make it go on to that */
    final def chain[B >: T](that : => Batch[B]) : Batch[B] = new Batch[B] {
        override implicit def executor = self.executor
        override def isLastBatch = false
        override def getMore() = if (self.isLastBatch) {
            Promise.successful(that)
        } else {
            self.anotherBatch().map(_.chain(that))
        }
        override def iterator() = self.iterator()
    }
}

trait AsyncCursor[+T] extends Closeable {
    self =>

    def close() : Unit

    def firstBatch() : Batch[T]

    final def blocking(timeout : Duration) : Cursor[T] = new Cursor[T] {
        override def close() = self.close()

        var batch : Batch[T] = self.firstBatch()
        var iter : Iterator[T] = batch.iterator()

        override def hasNext = iter.hasNext

        override def next() = {
            if (iter.hasNext) {
                iter.next()
            } else if (batch.isLastBatch) {
                throw new NoSuchElementException("Cursor has no next element")
            } else {
                batch = Await.result(batch.anotherBatch(), timeout)
                iter = batch.iterator()
                next()
            }
        }
    }

    final def map[B](f : T => B) : AsyncCursor[B] = new AsyncCursor[B] {
        override def close() = self.close()
        override def firstBatch() = self.firstBatch().map(f)
    }

    final def foreach[U](f : T => U) : Unit = {
        AsyncCursor.foreach(self.firstBatch(), f)
    }

    final def flatMap[B](f : T => GenTraversableOnce[B]) : AsyncCursor[B] = new AsyncCursor[B] {
        override def close() = self.close()
        override def firstBatch() = self.firstBatch().flatMap(f)
    }

    def ++[B >: T](that : => AsyncCursor[B]) : AsyncCursor[B] = new AsyncCursor[B] {
        override def close() : Unit = {
            self.close()
            that.close()
        }

        override def firstBatch() = {
            self.firstBatch().chain(that.firstBatch())
        }
    }
}

object AsyncCursor {

    private def foreach[T, U](batch : Batch[T], f : T => U) : Unit = {
        batch.foreach(f)
        if (!batch.isLastBatch) {
            batch.anotherBatch().foreach(foreach(_, f))
        }
    }
}
