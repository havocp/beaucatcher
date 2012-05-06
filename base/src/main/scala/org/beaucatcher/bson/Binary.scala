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

case class Binary(val data : Array[Byte], val subtype : BsonSubtype.Value) {

    def length = data.length

    // We have to fix equals() because default doesn't implement it
    // correctly (does not consider the contents of the byte[])
    override def equals(other : Any) : Boolean = {
        other match {
            case that : Binary =>
                (that canEqual this) &&
                    (subtype == that.subtype) &&
                    (data.length == that.data.length) &&
                    (data sameElements that.data)
            case _ => false
        }
    }

    // have to make hashCode match equals (array hashCode doesn't
    // look at elements, Seq hashCode does)
    override def hashCode() : Int = {
        41 * (41 + subtype.hashCode) + (data : Seq[Byte]).hashCode
    }

    private def bytesAsString(sb : StringBuilder, i : Traversable[Byte]) = {
        for (b <- i) {
            sb.append("%02x".format((b : Int) & 0xff))
        }
    }

    // default toString just shows byte[] object id
    override def toString() : String = {
        val sb = new StringBuilder
        sb.append("Binary(")
        val bytes = data.take(10)
        bytesAsString(sb, bytes)
        if (data.length > 10)
            sb.append("...")
        sb.append("@")
        sb.append(data.length.toString)
        sb.append(",")
        sb.append(subtype.toString)
        sb.append(")")
        sb.toString
    }
}

object Binary {
    def apply(data : Array[Byte]) : Binary = {
        Binary(data, BsonSubtype.GENERAL)
    }
}
