package org.beaucatcher.mongo

import org.beaucatcher.bson._
import org.beaucatcher.driver._
import akka.actor.ActorSystem
import scala.annotation.implicitNotFound
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.lang.reflect.InvocationTargetException

/**
 * A [[org.beaucatcher.mongo.Context]] represents an underlying Mongo protocol implementation, a connection
 * pool, and a single named database on that connection. It may also include other configuration settings
 * and the Akka ActorSystem.
 *
 * Implementations of [[org.beaucatcher.mongo.Context]] are provided by a "driver" such
 * as [[org.beaucatcher.jdriver.JavaDriverContext]].
 *
 * This object is normally passed around as an implicit.
 */
@implicitNotFound(msg = "No mongo.Context found")
trait Context {
    def underlyingConnection: Any
    def underlyingDatabase: Any
    def underlyingCollection(name: String): Any

    private lazy val _driver = {
        val klass = try {
            classLoader.loadClass(settings.driverClassName)
        } catch {
            case e: ClassNotFoundException =>
                throw new MongoException("Configured driver '" + settings.driverClassName + "' not found", e)
        }
        try {
            try {
                klass.getDeclaredConstructor(classOf[Config], classOf[ClassLoader]).newInstance(config, classLoader).asInstanceOf[Driver]
            } catch {
                case ite: InvocationTargetException =>
                    throw ite.getCause()
            }
        } catch {
            case e: Exception =>
                throw new MongoException("Failed to instantiate '" + settings.driverClassName + "': " + e.getClass.getSimpleName + ": " + e.getMessage, e)
        }
    }

    final def driver: Driver = _driver

    private lazy val _driverContext = driver.newContext(settings.uri, actorSystem)

    private[beaucatcher] def driverContext: DriverContext =
        _driverContext

    def database: Database

    def config: Config

    private lazy val _settings = new ContextSettings(config)

    private[beaucatcher] def settings: ContextSettings =
        _settings

    def actorSystem: ActorSystem

    /**
     * Should close the connection to Mongo; will break any code currently trying to use this context,
     * or any collection or database objects that point to this context.
     */
    def close: Unit

    def classLoader: ClassLoader
}

trait ContextProvider {
    def mongoContext: Context
}

object Context {
    def apply(config: Config, system: ActorSystem, loader: ClassLoader): Context = {
        val outerConfig = config
        new Context() {
            override lazy val database = Database(this)
            override val config = outerConfig
            override val actorSystem = system
            override def close(): Unit = driverContext.close()
            override def classLoader = loader

            override def underlyingConnection = driverContext.underlyingConnection
            override def underlyingDatabase = driverContext.underlyingDatabase
            override def underlyingCollection(name: String) = driverContext.underlyingCollection(name)
        }
    }

    private def defaultLoader = Thread.currentThread().getContextClassLoader()

    def apply(config: Config, system: ActorSystem): Context = {
        Context(config, system, defaultLoader)
    }

    def apply(system: ActorSystem): Context = {
        // ideally we'd use the actor system's class loader here but
        // it doesn't look possible to get at it?
        Context(system.settings.config, system)
    }

    def apply(system: ActorSystem, loader: ClassLoader): Context = {
        Context(system.settings.config, system, loader)
    }

    private lazy val systemCount = new java.util.concurrent.atomic.AtomicInteger(1)

    private def newSystem(config: Config, loader: ClassLoader) = ActorSystem("beaucatcher_" + systemCount.getAndIncrement,
        config, loader)

    def apply(config: Config): Context = {
        Context(config, newSystem(config, defaultLoader), defaultLoader)
    }

    def apply(config: Config, loader: ClassLoader): Context = {
        Context(config, newSystem(config, loader), loader)
    }

    def apply(): Context = {
        Context(ConfigFactory.load())
    }

    def apply(loader: ClassLoader): Context = {
        Context(ConfigFactory.load(), loader)
    }
}
