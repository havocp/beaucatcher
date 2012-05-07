package org.beaucatcher.channel.netty

import java.util.Date
import org.beaucatcher.bson._
import org.junit._

class BObjectCodecsTest extends TestUtils {
    import SerializerTest._

    @Test
    def roundTripBObject(): Unit = {
        import BObjectCodecs._

        testRoundTrip(BsonTest.makeObjectManyTypes())
    }
}
