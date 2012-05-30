package org.beaucatcher.channel.netty

import org.beaucatcher.bobject._
import org.beaucatcher.bson.TestUtils
import org.junit.Test

class BObjectCodecsTest extends TestUtils {

    @Test
    def roundTripBObject(): Unit = {
        import BObjectCodecs._

        SerializerTest.testRoundTrip(BObjectTest.makeObjectManyTypes())
    }
}
