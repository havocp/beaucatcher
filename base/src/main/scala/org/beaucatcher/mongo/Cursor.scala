package org.beaucatcher.mongo

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.util.Duration
import java.io.Closeable
import scala.collection.GenTraversableOnce
import scala.collection.Iterator

trait Cursor[+A] extends Iterator[A] with Closeable {
    def close(): Unit

    private class CursorOverIterator[+A](previous: Cursor[_], val iter: Iterator[A]) extends Cursor[A] {
        // allow GC of any intermediate cursors
        val orig: Cursor[_] = previous match {
            case c: CursorOverIterator[_] => c.orig
            case _ => previous
        }

        override def hasNext = iter.hasNext
        override def next() = iter.next
        override def close() = orig.close()
    }

    // This is annoying, but Iterator doesn't seem to do CanBuildFrom
    // so to keep the close() we have to override all these methods...

    override def map[B](f: A => B): Cursor[B] =
        new CursorOverIterator(this, super.map(f))

    override def flatMap[B](f: A => GenTraversableOnce[B]): Cursor[B] =
        new CursorOverIterator(this, super.flatMap(f))

    override def take(n: Int): Cursor[A] =
        new CursorOverIterator(this, super.take(n))

    override def drop(n: Int): Cursor[A] =
        new CursorOverIterator(this, super.drop(n))

    override def slice(from: Int, until: Int): Cursor[A] =
        new CursorOverIterator(this, super.slice(from, until))

    override def filter(p: A => Boolean): Cursor[A] =
        new CursorOverIterator(this, super.filter(p))

    override def withFilter(p: A => Boolean): Cursor[A] =
        new CursorOverIterator(this, super.withFilter(p))

    override def filterNot(p: A => Boolean): Cursor[A] =
        new CursorOverIterator(this, super.filterNot(p))

    override def collect[B](pf: PartialFunction[A, B]): Cursor[B] =
        new CursorOverIterator(this, super.collect(pf))

    override def scanLeft[B](z: B)(op: (B, A) => B): Cursor[B] =
        new CursorOverIterator(this, super.scanLeft(z)(op))

    override def scanRight[B](z: B)(op: (A, B) => B): Cursor[B] =
        new CursorOverIterator(this, super.scanRight(z)(op))

    override def takeWhile(p: A => Boolean): Cursor[A] =
        new CursorOverIterator(this, super.takeWhile(p))

    override def dropWhile(p: A => Boolean): Cursor[A] =
        new CursorOverIterator(this, super.dropWhile(p))

    override def zip[B](that: Iterator[B]): Cursor[(A, B)] =
        new CursorOverIterator(this, super.zip(that))
}

trait Batch[+T] extends Traversable[T] {
    self =>

    /** Is this the isLastBatch batch? */
    def isLastBatch: Boolean

    protected[this] def getMore(): Future[Batch[T]]

    private lazy val _more = getMore()

    /** If not isLastBatch, we can fetch the subsequent one */
    final def anotherBatch(): Future[Batch[T]] = if (isLastBatch) {
        Promise.failed(new MongoException("No more batches, cursor exhausted"))
    } else {
        _more
    }

    protected[beaucatcher] implicit def executor: ExecutionContext

    // this has to make a new iterator each time
    protected[beaucatcher] def iterator(): Iterator[T]

    final override def foreach[U](f: T => U): Unit = iterator().foreach(f)
    final override def isEmpty: Boolean = iterator().isEmpty
    final override def hasDefiniteSize: Boolean = true
    final override def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) = iterator.copyToArray(xs, start, len)
    final override def find(p: T => Boolean): Option[T] = iterator().find(p)
    final override def exists(p: T => Boolean): Boolean = iterator().exists(p)
    final override def forall(p: T => Boolean): Boolean = iterator().forall(p)
    final override def seq: Batch[T] = self
    final override def toTraversable: Traversable[T] = iterator().toTraversable
    final override def toIterator: Iterator[T] = iterator()
    final override def toStream: Stream[T] = iterator().toStream

    final def map[B](f: T => B): Batch[B] = new Batch[B]() {
        override implicit def executor = self.executor
        override def isLastBatch = self.isLastBatch
        override def getMore() = self.anotherBatch().map(_.map(f))
        override def iterator() = self.iterator().map(f)
    }

    final def flatMap[B](f: T => GenTraversableOnce[B]): Batch[B] = new Batch[B]() {
        override implicit def executor = self.executor
        override def isLastBatch = self.isLastBatch
        override def getMore() = self.anotherBatch().map(_.flatMap(f))
        override def iterator() = self.iterator().flatMap(f)
    }

    /** When getMore() gets to the isLastBatch batch, make it go on to that */
    final def chain[B >: T](that: => Batch[B]): Batch[B] = new Batch[B] {
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

    def close(): Unit

    def firstBatch(): Batch[T]

    final def blocking(timeout: Duration): Cursor[T] = new Cursor[T] {
        override def close() = self.close()

        var batch: Batch[T] = self.firstBatch()
        var iter: Iterator[T] = batch.iterator()

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

    final def map[B](f: T => B): AsyncCursor[B] = new AsyncCursor[B] {
        override def close() = self.close()
        override def firstBatch() = self.firstBatch().map(f)
    }

    final def foreach[U](f: T => U): Unit = {
        AsyncCursor.foreach(self.firstBatch(), f)
    }

    final def flatMap[B](f: T => GenTraversableOnce[B]): AsyncCursor[B] = new AsyncCursor[B] {
        override def close() = self.close()
        override def firstBatch() = self.firstBatch().flatMap(f)
    }

    def ++[B >: T](that: => AsyncCursor[B]): AsyncCursor[B] = new AsyncCursor[B] {
        override def close(): Unit = {
            self.close()
            that.close()
        }

        override def firstBatch() = {
            self.firstBatch().chain(that.firstBatch())
        }
    }
}

object AsyncCursor {

    private def foreach[T, U](batch: Batch[T], f: T => U): Unit = {
        batch.foreach(f)
        if (!batch.isLastBatch) {
            batch.anotherBatch().foreach(foreach(_, f))
        }
    }

    private class SingleBatch[+T](orig: TraversableOnce[T])(implicit override val executor: ExecutionContext) extends Batch[T] {
        val all = orig.toSeq
        override def isLastBatch = true

        override def getMore(): Future[Batch[T]] = throw new BugInSomethingMongoException("shouldn't have been called")

        override def iterator(): Iterator[T] = all.iterator
    }

    private[beaucatcher] def apply[T](orig: Cursor[T])(implicit executor: ExecutionContext): AsyncCursor[T] = new AsyncCursor[T] {
        override def close() = orig.close()
        override def firstBatch = new SingleBatch(orig)
    }
}
