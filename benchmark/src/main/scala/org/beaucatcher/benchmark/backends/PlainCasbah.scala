package org.beaucatcher.benchmark.backends

import java.util.concurrent.Executors
import scala.collection.Map
import scala.collection.TraversableOnce
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.util.bson.conversions.RegisterJodaTimeConversionHelpers
import org.beaucatcher.benchmark.MongoBenchmark
import org.beaucatcher.benchmark.BenchmarkFuture
import java.util.concurrent.ExecutorService

class PlainCasbahBenchmark extends MongoBenchmark[MongoCollection] {
    override val name = "Casbah"

    private var connection : MongoConnection = null
    private var db : MongoDB = null
    private var threads : ExecutorService = null

    override def openDatabase(host : String, port : Int, dbname : String) : Unit = {
        // goal is to have enough threads that we don't bottleneck. resource usage doesn't matter.
        threads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors * 2)

        RegisterJodaTimeConversionHelpers()

        connection = MongoConnection(host, port)
        connection.setWriteConcern(WriteConcern.Safe)

        connection.dropDatabase(dbname)

        db = connection(dbname)
    }

    override def cleanupDatabase() : Unit = {
        val name = db.name
        connection.dropDatabase(name)

        threads.shutdown()
        threads = null
    }

    private def convertObject(obj : Map[String, Any]) : MongoDBObject = {
        val builder = MongoDBObject.newBuilder
        for (kv <- obj.iterator) {
            kv match {
                case (k, v : Map[_, _]) =>
                    builder += Pair(k, convertObject(v.asInstanceOf[Map[String, Any]]))
                case _ =>
                    builder += kv
            }
        }
        builder.result
    }

    override def createCollection(collectionName : String, data : TraversableOnce[Map[String, Any]]) : MongoCollection = {
        val c = db(collectionName)
        require(c.count == 0)
        for (obj <- data) {
            c.insert(convertObject(obj))
        }
        c
    }

    override def findOne(collection : MongoCollection) = {
        val maybeOne = collection.findOne()
        // throw if it's not there
        maybeOne.get
    }

    override def findAll(collection : MongoCollection, numberExpected : Int) = {
        val all = collection.find()
        // force the cursor to all come through
        // (done imperative-style!)
        var num = 0
        while (all.hasNext) {
            all.next
            num += 1
        }
        require(numberExpected == num)
    }

    override def findAllAsync(collection : MongoCollection, numberExpected : Int) : BenchmarkFuture = {
        BenchmarkFuture(threads.submit(new Runnable() {
            override def run() = {
                findAll(collection, numberExpected)
            }
        }))
    }

    override def findOneAsync(collection : MongoCollection) : BenchmarkFuture = {
        BenchmarkFuture(threads.submit(new Runnable() {
            override def run() = {
                findOne(collection)
            }
        }))
    }
}
