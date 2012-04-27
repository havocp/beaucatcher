package org.beaucatcher.jdriver

import org.beaucatcher.bson._
import org.beaucatcher.wire._
import org.beaucatcher.mongo._
import com.mongodb.DBObject
import org.bson.BSONObject

/** Concrete implicit encoders/decoders for mongo-java-driver */
private[beaucatcher] object Support {

    implicit def bobjectQueryEncodeSupport : QueryEncodeSupport[BObject] =
        BObjectEncodeSupport

    implicit def bobjectEntityEncodeSupport : EntityEncodeSupport[BObject] =
        BObjectEncodeSupport

    implicit def bobjectEntityDecodeSupport : EntityDecodeSupport[BObject] =
        BObjectDecodeSupport

    private object BObjectEncodeSupport
        extends JavaEncodeSupport[BObject]
        with QueryEncodeSupport[BObject]
        with EntityEncodeSupport[BObject] {

        override def toDBObject(bobj : BObject) : DBObject = {
            new BObjectDBObject(bobj)
        }
    }

    private object BObjectDecodeSupport
        extends JavaDecodeSupport[BObject]
        with EntityDecodeSupport[BObject] {
        override def fromBsonObject(obj : BSONObject) : BObject = {
            import Implicits._
            return obj
        }
    }
}
