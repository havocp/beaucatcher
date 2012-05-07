package org.beaucatcher.channel.netty

import java.util.Date
import org.beaucatcher.bson._
import org.junit._

case class JustAnInt(i: Int)

case class JustAnOptionalInt(i: Option[Int])

case class ManyTypes(n: AnyRef,
    i: Int,
    l: Long,
    d: Double,
    b: Boolean,
    s: String,
    date: Date,
    t: Timestamp,
    oid: ObjectId,
    bin: Binary,
    list: Seq[Any])

class CaseClassCodecsTest extends TestUtils {
    import SerializerTest._

    @Test
    def roundTripJustAnInt(): Unit = {
        val example = JustAnInt(42)
        val codecs = CaseClassCodecs[JustAnInt]()
        import codecs._
        implicit val queryEncoder = codecs.queryEncoder

        testRoundTrip(example)
    }

    @Test
    def roundTripJustAnIntOption(): Unit = {
        val codecs = CaseClassCodecs[JustAnOptionalInt]()
        import codecs._
        implicit val queryEncoder = codecs.queryEncoder

        testRoundTrip(JustAnOptionalInt(Some(42)))
        testRoundTrip(JustAnOptionalInt(None))
    }

    @Test
    def roundTripCaseClass(): Unit = {
        val example = ManyTypes(null, 1, 2, 3.14, true, "quick brown fox",
            BsonTest.someJavaDate, Timestamp((BsonTest.someJavaDate.getTime / 1000).toInt, 1),
            ObjectId(), Binary(new Array[Byte](10), BsonSubtype.GENERAL),
            Seq(1, 2, 3, 4, "hello"))
        val codecs = CaseClassCodecs[ManyTypes]()
        import codecs._
        implicit val queryEncoder = codecs.queryEncoder

        testRoundTrip(example)
    }
}
