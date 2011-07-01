package org.beaucatcher

import org.bson.collection._
import akka.dispatch._
import java.util.concurrent.TimeUnit

package object hammersmith {
    private[hammersmith] def newPromise[T] = {
        // the default timeout appears to be zero, so we have to fix this
        new DefaultCompletableFuture[T](15, TimeUnit.SECONDS)
    }

    private[hammersmith] implicit object SerializableBSONDocument extends SerializableBSONDocumentLike[BSONDocument]
}
