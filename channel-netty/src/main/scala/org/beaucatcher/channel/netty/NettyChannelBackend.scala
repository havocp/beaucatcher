package org.beaucatcher.channel.netty

import akka.dispatch._
import org.beaucatcher.channel._
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.ByteOrder
import com.typesafe.config.Config
import org.beaucatcher.mongo.BugInSomethingMongoException

// this class is renamed by the build to substitute XX with a version
class NettyXXChannelBackend(config: Config) extends ChannelBackend {

    private def checkNettyVersion(name: String, version: String): Unit = {
        val nettyVersion = org.jboss.netty.util.Version.ID
        if (!nettyVersion.startsWith(version))
            throw new BugInSomethingMongoException("Configured channel backend for " + version + " " + name + " but Netty on classpath is " + nettyVersion)

        val noDot = version.substring(0, 1) + version.substring(2, 3)
        val other = noDot match {
            case "33" => "34"
            case "34" => "33"
        }
        val otherClassName = name.replace(noDot, other)
        val otherClassFound = try {
            getClass.getClassLoader.loadClass(otherClassName) != null
        } catch {
            case e: ClassNotFoundException =>
                false
        }
        if (otherClassFound) {
            // the two backend jars don't have different names for all their internal
            // types, so they can't coexist
            throw new BugInSomethingMongoException("Cannot have " + otherClassName + " and " + name + " both on the classpath, remove one of the jars")
        }
    }

    protected def checkClassName(name: String): Unit = name match {
        case name if name.contains("Netty33ChannelBackend") => checkNettyVersion(name, "3.3")
        case name if name.contains("Netty34ChannelBackend") => checkNettyVersion(name, "3.4")
        case name if name.contains("NettyXXChannelBackend") => // no checks, we're testing in an IDE or something
        case name if name.contains("NettyChannelBackend") => // no checks, this is the alias subclass
        case name =>
            throw new BugInSomethingMongoException("Unhandled NettyChannelBackend class name: " + name)
    }

    checkClassName(getClass.getName)

    override def newSocketFactory()(implicit executionContext: ExecutionContext) =
        new NettyMongoSocketFactory()

    override def newDynamicEncodeBuffer(preallocateSize: Int) =
        Buffer(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, preallocateSize))
}

/** An alias for the versioned name, usable in config files */
class NettyChannelBackend(config: Config) extends NettyXXChannelBackend(config) {
    checkClassName(getClass.getSuperclass.getName)
}
