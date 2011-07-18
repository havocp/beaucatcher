package org.beaucatcher.benchmark.backends

import java.util.concurrent.Executors
import scala.collection.Map
import scala.collection.TraversableOnce
import org.beaucatcher.benchmark.MongoBenchmark
import org.beaucatcher.benchmark.BenchmarkFuture
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.Mongo
import com.mongodb.WriteConcern
import java.util.concurrent.ExecutorService

class RawJavaBenchmark extends MongoBenchmark[DBCollection] {
    override val name = "Java Driver"

    private var connection : Mongo = null
    private var db : DB = null
    private var threads : ExecutorService = null

    override def openDatabase(host : String, port : Int, dbname : String) : Unit = {
        // goal is to have enough threads that we don't bottleneck. resource usage doesn't matter.
        threads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors * 2)

        connection = new Mongo(host, port)
        connection.setWriteConcern(WriteConcern.SAFE)

        connection.dropDatabase(dbname)

        db = connection.getDB(dbname)
    }

    override def cleanupDatabase() : Unit = {
        val name = db.getName
        connection.dropDatabase(name)

        threads.shutdown()
        threads = null
    }

    private def convertObject(obj : Map[String, Any]) : BasicDBObject = {
        val dbobj = new BasicDBObject()
        for (kv <- obj.iterator) {
            kv match {
                case (k, v : Map[_, _]) =>
                    dbobj.put(k, convertObject(v.asInstanceOf[Map[String, Any]]))
                case (k, v) =>
                    dbobj.put(k, v)
            }
        }
        dbobj
    }

    override def createCollection(collectionName : String, data : TraversableOnce[Map[String, Any]]) : DBCollection = {
        val c = db.getCollection(collectionName)
        require(c.count() == 0)
        for (obj <- data) {
            c.insert(convertObject(obj))
        }
        c
    }

    override def findOne(collection : DBCollection) = {
        val one = collection.findOne()
        require(one != null)
    }

    override def findAll(collection : DBCollection, numberExpected : Int) = {
        val all = collection.find()
        // force the cursor to all come through
        var num = 0
        while (all.hasNext) {
            all.next
            num += 1
        }
        require(numberExpected == num)
    }

    override def findAllAsync(collection : DBCollection, numberExpected : Int) : BenchmarkFuture = {
        BenchmarkFuture(threads.submit(new Runnable() {
            override def run() = {
                findAll(collection, numberExpected)
            }
        }))
    }

    override def findOneAsync(collection : DBCollection) : BenchmarkFuture = {
        BenchmarkFuture(threads.submit(new Runnable() {
            override def run() = {
                findOne(collection)
            }
        }))
    }
}
