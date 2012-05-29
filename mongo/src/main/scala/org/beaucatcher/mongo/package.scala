package org.beaucatcher

import org.beaucatcher.bson._

package object mongo {

    private[mongo] object AssertNotUsedEncoder
        extends QueryEncoder[Any]
        with UpsertEncoder[Any]
        with ModifierEncoder[Any] {
        private def unused(): Exception =
            new BugInSomethingMongoException("encoder should not have been used")

        override def encode(buf: EncodeBuffer, t: Any): Unit = throw unused()
        override def encodeIterator(t: Any): Iterator[(String, Any)] = throw unused()
    }
}
