package org.beaucatcher.bson

import org.bson.{ types => j }

object JavaConversions {
    implicit def asJavaObjectId(o : ObjectId) = new j.ObjectId(o.time, o.machine, o.inc)

    implicit def asScalaObjectId(o : j.ObjectId) = ObjectId(o.getTimeSecond(), o.getMachine(), o.getInc())

    implicit def asJavaTimestamp(t : Timestamp) = new j.BSONTimestamp(t.time, t.inc)

    implicit def asScalaTimestamp(t : j.BSONTimestamp) = Timestamp(t.getTime(), t.getInc())

    implicit def asJavaBinary(b : Binary) = new j.Binary(BsonSubtype.toByte(b.subtype), b.data)

    implicit def asScalaBinary(b : j.Binary) = Binary(b.getData(), BsonSubtype.fromByte(b.getType()).get)
}
