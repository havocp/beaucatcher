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

//import java.nio.ByteBuffer;

/**
 * A buffer we can encode data to. This is intended to abstract
 * e.g. Netty ChannelBuffer vs. ByteBuffer vs. whatever else.
 * Of course ChannelBuffer is already such an abstraction, but this
 * creates an isolation layer vs. Netty ABI changes. This class
 * is not the right place for any BSON-specific stuff.
 */
trait EncodeBuffer {

    def writerIndex: Int

    def ensureWritableBytes(writableBytes: Int): Unit

    def setInt(index: Int, value: Int): Unit

    def writeBoolean(value: Boolean): Unit

    def writeByte(value: Byte): Unit

    def writeInt(value: Int): Unit

    def writeLong(value: Long): Unit

    def writeDouble(value: Double): Unit

    def writeBytes(src: Array[Byte]): Unit

    def writeBytes(src: ByteBuffer): Unit

    // does not increase readerIndex on src
    // (note: ChannelBuffer.writeBytes(buf) in netty DOES increase readerIndex)
    def writeBuffer(src: EncodeBuffer): Unit

    // does not increase readerIndex on src
    def writeBuffer(src: EncodeBuffer, srcIndex: Int, length: Int)

    // note: NOT REQUIRED to copy, may share content with EncodeBuffer
    def toByteBuffer(): ByteBuffer

    // note: NOT REQUIRED to copy, may share content with EncodeBuffer
    def toDecodeBuffer(): DecodeBuffer

}
