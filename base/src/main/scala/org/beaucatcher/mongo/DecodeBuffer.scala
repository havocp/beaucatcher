/*
 * Based on ChannelBuffer.java Copyright 2011 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.beaucatcher.mongo
import java.nio.ByteBuffer

/** See EncodeBuffer for a description. */
trait DecodeBuffer {

    def readerIndex: Int

    def readableBytes: Int

    def readBoolean(): Boolean

    def readByte(): Byte

    def readInt(): Int

    def readLong(): Long

    def readDouble(): Double

    def readBytes(dst: Array[Byte]): Unit

    def skipBytes(length: Int): Unit

    def bytesBefore(value: Byte): Int

    // not required to make a copy, may share content
    def toByteBuffer(): ByteBuffer
}
