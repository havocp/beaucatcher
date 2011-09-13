/**
 *   Copyright (C) 2011 Havoc Pennington
 *
 *   Derived in part from mongo-java-driver,
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

package org.beaucatcher.mongo

package object wire {
    /**
     * Tailable means cursor is not closed when the last data is retrieved.
     * Rather, the cursor marks the final object's position.
     * You can resume using the cursor later, from where it was located, if more data were received.
     * Like any "latent cursor", the cursor may become invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
     */
    val QUERYOPTION_TAILABLE = 1 << 1
    /**
     * When turned on, read queries will be directed to slave servers instead of the primary server.
     */
    val QUERYOPTION_SLAVEOK = 1 << 2
    /**
     * Internal replication use only - driver should not set
     */
    val QUERYOPTION_OPLOGREPLAY = 1 << 3
    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.
     * Set this option to prevent that.
     */
    val QUERYOPTION_NOTIMEOUT = 1 << 4
    /**
     * Use with TailableCursor.
     * If we are at the end of the data, block for a while rather than returning no data.
     * After a timeout period, we do return as normal.
     */
    val QUERYOPTION_AWAITDATA = 1 << 5
    /**
     * Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried.
     * Faster when you are pulling a lot of data and know you want to pull it all down.
     * Note: the client is not allowed to not read all the data unless it closes the connection.
     */
    val QUERYOPTION_EXHAUST = 1 << 6

    /**
     * Set when getMore is called but the cursor id is not valid at the server.
     * Returned with zero results.
     */
    val RESULTFLAG_CURSORNOTFOUND = 1
    /**
     * Set when query failed.
     * Results consist of one document containing an "$err" field describing the failure.
     */
    val RESULTFLAG_ERRSET = 2
    /**
     * Drivers should ignore this.
     * Only mongos will ever see this set, in which case, it needs to update config from the server.
     */
    val RESULTFLAG_SHARDCONFIGSTALE = 4
    /**
     * Set when the server supports the AwaitData Query option.
     * If it doesn't, a client should sleep a little between getMore's of a Tailable cursor.
     * Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.
     */
    val RESULTFLAG_AWAITCAPABLE = 8

    implicit def queryFlagsAsInt(flags : Set[QueryFlag]) : Int = {
        var i = 0
        for (f <- flags) {
            val o = f match {
                case QueryAwaitData => QUERYOPTION_AWAITDATA
                case QueryExhaust => QUERYOPTION_EXHAUST
                case QueryNoTimeout => QUERYOPTION_NOTIMEOUT
                case QueryOpLogReplay => QUERYOPTION_OPLOGREPLAY
                case QuerySlaveOk => QUERYOPTION_SLAVEOK
                case QueryTailable => QUERYOPTION_TAILABLE
            }
            i |= o
        }
        i
    }
}
