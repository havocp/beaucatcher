package org.beaucatcher

import org.beaucatcher.bson._
import org.beaucatcher.mongo._

package object driver {

    private[beaucatcher] def defaultIndexName[Q](keys: Q)(implicit encoder: QueryEncoder[Q]): String = {
        val sb = new StringBuilder()
        for (kv <- encoder.encodeIterator(keys)) {
            if (sb.length > 0)
                sb.append("_")
            sb.append(kv._1)
            sb.append("_")
            kv._2 match {
                case null =>
                    throw new BugInSomethingMongoException("Index object had a null value for " + kv._1)
                case n: Number =>
                    sb.append(n.intValue.toString)
                case s: String =>
                    sb.append(s.replace(' ', '_'))
                case _ =>
                    throw new BugInSomethingMongoException("Index object had %s:%s but only numbers and strings were expected as values".format(kv._1, kv._2))
            }
        }
        sb.toString
    }

    private[beaucatcher] implicit def queryFlagsAsInt(flags: Set[QueryFlag]): Int = {
        import org.beaucatcher.wire.Mongo

        var i = 0
        for (f <- flags) {
            val o = f match {
                case QueryAwaitData => Mongo.QUERY_FLAG_AWAIT_DATA
                case QueryExhaust => Mongo.QUERY_FLAG_EXHAUST
                case QueryNoTimeout => Mongo.QUERY_FLAG_NO_CURSOR_TIMEOUT
                case QueryOpLogReplay => Mongo.QUERY_FLAG_OPLOG_RELAY
                case QuerySlaveOk => Mongo.QUERY_FLAG_SLAVE_OK
                case QueryTailable => Mongo.QUERY_FLAG_TAILABLE_CURSOR
            }
            i |= o
        }
        i
    }
}
