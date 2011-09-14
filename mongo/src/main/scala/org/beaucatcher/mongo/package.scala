package org.beaucatcher

import org.beaucatcher.bson._

package object mongo {

    private[mongo] def defaultIndexName(keys : BObject) : String = {
        val sb = new StringBuilder()
        for (kv <- keys.iterator) {
            if (sb.length > 0)
                sb.append("_")
            sb.append(kv._1)
            sb.append("_")
            kv._2 match {
                case n : BNumericValue[_] =>
                    sb.append(n.intValue.toString)
                case BString(s) =>
                    sb.append(s.replace(' ', '_'))
                case _ =>
                    throw new BugInSomethingMongoException("Index object had %s:%s but only numbers and strings were expected as values".format(kv._1, kv._2))
            }
        }
        sb.toString
    }
}
