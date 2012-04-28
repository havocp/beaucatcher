package org.beaucatcher

import org.beaucatcher.bson._

package object mongo {

    /**
     * A sync (blocking) collection parameterized to work with BObject
     */
    type BObjectSyncCollection[IdType] = SyncCollection[BObject, BObject, IdType, BValue]

    /**
     * An async (nonblocking) collection parameterized to work with BObject
     */
    type BObjectAsyncCollection[IdType] = AsyncCollection[BObject, BObject, IdType, BValue]

    /**
     * A sync (blocking) collection parameterized to support returning case class entities.
     */
    type CaseClassSyncCollection[OuterQueryType, EntityType <: Product, IdType] = EntitySyncCollection[OuterQueryType, EntityType, IdType]

    /**
     * An async (nonblocking) collection parameterized to support returning case class entities.
     */
    type CaseClassAsyncCollection[OuterQueryType, EntityType <: Product, IdType] = EntityAsyncCollection[OuterQueryType, EntityType, IdType]

    /**
     * A sync (blocking) collection parameterized to support returning some kind of domain object ("entity").
     */
    type EntitySyncCollection[OuterQueryType, EntityType <: AnyRef, IdType] = SyncCollection[OuterQueryType, EntityType, IdType, Any]

    /**
     * An async (nonblocking) collection parameterized to support returning some kind of domain object ("entity").
     */
    type EntityAsyncCollection[OuterQueryType, EntityType <: AnyRef, IdType] = AsyncCollection[OuterQueryType, EntityType, IdType, Any]

    private[beaucatcher] def defaultIndexName(keys : BObject) : String = {
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

    private[beaucatcher] implicit def queryFlagsAsInt(flags : Set[QueryFlag]) : Int = {
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
