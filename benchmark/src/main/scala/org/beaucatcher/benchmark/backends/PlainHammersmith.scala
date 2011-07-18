package org.beaucatcher.benchmark.backends

import java.util.concurrent.Executors
import scala.collection.Map
import scala.collection.TraversableOnce
import org.beaucatcher.benchmark.MongoBenchmark
import org.beaucatcher.benchmark.BenchmarkFuture
import com.mongodb.async._
import java.util.concurrent.CountDownLatch
import org.bson.collection._
import com.mongodb.async.futures.RequestFutures

class PlainHammersmithBenchmark extends MongoBenchmark[Collection] {
    override val name = "Hammersmith"

    private var connection : MongoConnection = null
    private var db : DB = null

    private def dropDatabase(dropMe : DB) = {
        val latch = new CountDownLatch(1)
        dropMe.dropDatabase()({ result =>
            require(result)
            latch.countDown()
        })
        latch.await()
    }

    override def openDatabase(host : String, port : Int, dbname : String) : Unit = {
        connection = MongoConnection(host, port)
        connection.writeConcern = WriteConcern.Safe

        db = connection(dbname)
        dropDatabase(db)
    }

    override def cleanupDatabase() : Unit = {
        dropDatabase(db)
        db = null
        connection = null
    }

    private def convertObject(obj : Map[String, Any]) : Document = {
        val builder = Document.newBuilder
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

    private def count(collection : Collection) : Int = {
        var theCount = -1
        val latch = new CountDownLatch(1)
        collection.count()({ result =>
            theCount = result
            latch.countDown()
        })
        latch.await()
        require(theCount > -1)
        theCount
    }

    override def createCollection(collectionName : String, data : TraversableOnce[Map[String, Any]]) : Collection = {
        val c = db(collectionName)
        require(count(c) == 0)
        val dataList = data.toList
        val latch = new CountDownLatch(dataList.length)
        for (obj <- dataList) {
            c.insert(convertObject(obj))(RequestFutures.write({ result =>
                latch.countDown()
            }))
        }
        latch.await()
        c
    }

    override def findOne(collection : Collection) = {
        var maybeOne : Option[Document] = None
        val latch = new CountDownLatch(1)
        collection.findOne()(RequestFutures.findOne[Document]({ either =>
            maybeOne = either.right.toOption
            latch.countDown()
        }))
        latch.await()
        // throw if it's not there
        maybeOne.get
    }

    override def findAll(collection : Collection, numberExpected : Int) = {
        var numFound = 0
        val latch = new CountDownLatch(1)
        collection.find()(RequestFutures.query[Document]({ either =>
            val cursor = either.right.get
            while (cursor.hasMore) {
                cursor.next() match {
                    case Cursor.Entry(_) =>
                        numFound += 1
                    case Cursor.Empty => {
                        val batchLatch = new CountDownLatch(1)
                        cursor.nextBatch({ () => batchLatch.countDown() })
                        batchLatch.countDown()
                    }
                }
            }
            latch.countDown()
        }))

        latch.await()

        require(numberExpected == numFound)
    }

    override def findAllAsync(collection : Collection, numberExpected : Int) : BenchmarkFuture = {
        val latch = new CountDownLatch(1)
        collection.find()(RequestFutures.query[Document]({ either =>
            var numFound = 0
            val cursor = either.right.get
            while (cursor.hasMore) {
                cursor.next() match {
                    case Cursor.Entry(_) =>
                        numFound += 1
                    case Cursor.Empty => {
                        val batchLatch = new CountDownLatch(1)
                        cursor.nextBatch({ () => batchLatch.countDown() })
                        batchLatch.countDown()
                    }
                    case Cursor.EOF =>
                        require(numberExpected == numFound)
                }
            }
            require(numberExpected == numFound)
            latch.countDown()
        }))
        BenchmarkFuture(latch)
    }

    override def findOneAsync(collection : Collection) : BenchmarkFuture = {
        val latch = new CountDownLatch(1)
        collection.findOne()(RequestFutures.findOne[Document]({ either =>
            latch.countDown()
        }))
        BenchmarkFuture(latch)
    }
}
