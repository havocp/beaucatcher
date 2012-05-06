/**
 * Copyright (C) 2011 Havoc Pennington
 *
 * Derived from mongo-java-driver,
 *
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.beaucatcher.bson

import java.util.Date

/**
 * this is used for internal increment values.
 * for storing normal dates in MongoDB, you should use java.util.Date.
 * The "time" is in seconds since epoch while the "inc" is just an
 * incrementing serial.
 */
case class Timestamp(time: Int, inc: Int) {
    def timeMillis = time * 1000L
    def date = new Date(timeMillis)
    /**
     * convert to milliseconds and treat the "increment" as milliseconds after
     * the round number of seconds.
     */
    def asLong = timeMillis | inc
}

object Timestamp {
    private lazy val _zero = Timestamp(0, 0)
    def zero: Timestamp = _zero

    def fromNumber(n: Number) = Timestamp((n.longValue / 1000).intValue, (n.longValue % 1000).intValue)
}
