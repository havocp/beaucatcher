package org.beaucatcher

package object util {
    private[beaucatcher] def toHex(bytes: Array[Byte]): String = {
        val buf = new StringBuilder(bytes.length * 2)

        for (b <- bytes) {
            val x: Int = b & 0xFF
            val s = Integer.toHexString(x)
            if (s.length == 1)
                buf.append("0")
            buf.append(s)
        }

        buf.toString
    }
}
