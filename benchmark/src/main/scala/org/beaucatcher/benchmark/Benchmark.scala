package org.beaucatcher.benchmark

import scala.collection._
import java.util.concurrent.TimeUnit

trait BenchmarkFuture {
    def await() : Unit
}

object BenchmarkFuture {
    private class BenchmarkFutureFromJava(val f : java.util.concurrent.Future[_])
        extends BenchmarkFuture {
        override def await() = { f.get(1000, TimeUnit.SECONDS) }
    }

    def apply(f : java.util.concurrent.Future[_]) : BenchmarkFuture = {
        new BenchmarkFutureFromJava(f)
    }

    private class BenchmarkFutureFromLatch(val latch : java.util.concurrent.CountDownLatch)
        extends BenchmarkFuture {
        override def await() = { latch.await(1000, TimeUnit.SECONDS) }
    }

    def apply(latch : java.util.concurrent.CountDownLatch) : BenchmarkFuture = {
        new BenchmarkFutureFromLatch(latch)
    }
}

/**
 * The benchmark should use Safe write concern mode, though the benchmark at
 * the moment is all reading anyway. (FIXME add writing.)
 *
 * The main point of this benchmark is to test the overhead of the Mongo
 * library, not to test the speed of Mongo itself; the focus is on
 * deserialization (and maybe in the future, serialization). So there's
 * no need to try a bunch of different kinds of query or anything, presumably.
 */
trait MongoBenchmark[T] {
    val name : String
    lazy val cleanName : String = name.toLowerCase.replace(" ", "")

    /** Open up and drop the database all test collections will be in. */
    def openDatabase(host : String, port : Int, dbname : String) : Unit

    /** Clean up, e.g. drop database */
    def cleanupDatabase() : Unit

    /** Return a handle for working with the given collection */
    def createCollection(collectionName : String, data : TraversableOnce[Map[String, Any]]) : T

    // this should find *and retrieve* all objects in the collection.
    // i.e. it must traverse the cursor. To make benchmarks match up,
    // by convention do this by just doing a foreach on the cursor.
    // please assert that numberExpected are in fact found
    def findAll(collection : T, numberExpected : Int) : Unit

    def findAllAsync(collection : T, numberExpected : Int) : BenchmarkFuture

    // retrieve a single object from the collection (the "first" one)
    def findOne(collection : T) : Unit

    def findOneAsync(collection : T) : BenchmarkFuture
}

object AlreadyCompletedBenchmarkFuture
    extends BenchmarkFuture {
    override def await() : Unit = {}
}

private class Benchmarker {
    private case class Benchmark(iterations : Int, requestsPerIteration : Int, name : String, body : () => Unit)
    private case class BenchmarkResult(name : String, timeNanosPerRequest : Long)

    private var benchmarks : List[Benchmark] = List()
    private var results : List[BenchmarkResult] = List()

    private def recordIteration(name : String, requestsPerIteration : Int, body : () => Unit) : Unit = {
        val start = System.nanoTime()
        body()
        val end = System.nanoTime()
        val nanosPerIteration = (end - start) / requestsPerIteration.toDouble
        results = BenchmarkResult(name, nanosPerIteration.toLong) :: results
    }

    def addBenchmark(iterations : Int, requestsPerIteration : Int, name : String, body : => Unit) : Unit = {
        benchmarks = Benchmark(iterations, requestsPerIteration, name, { () => body }) :: benchmarks
    }

    private def warmup(iterations : Int, name : String, body : () => Unit) : Unit = {
        val tenth = iterations / 10
        printf("  warming up '%s' ", name)
        System.out.flush()
        for (i <- 1 to iterations) {
            if ((i % tenth) == 0) {
                printf(".")
                System.out.flush()
            }
            body()
        }
        printf("done\n")
    }

    private def run(iterations : Int, requestsPerIteration : Int, name : String, body : () => Unit) : Unit = {
        val tenth = iterations / 10

        printf("  running '%s' ", name)
        System.out.flush()
        for (i <- 1 to iterations) {
            if ((i % tenth) == 0) {
                printf(".")
                System.out.flush()
            }
            recordIteration(name, requestsPerIteration, body)
        }
        printf("done\n")
    }

    def runAll() : Unit = {
        benchmarks = benchmarks.sortBy({ _.name })
        for (b <- benchmarks) {
            warmup(b.iterations, b.name, b.body)
        }
        for (b <- benchmarks) {
            run(b.iterations, b.requestsPerIteration, b.name, b.body)
        }
    }

    def output() : Unit = {
        val splitByName = results.foldLeft(Map.empty : Map[String, List[BenchmarkResult]])({ (sofar, result) =>
            val prev = sofar.getOrElse(result.name, Nil)
            sofar + Pair(result.name, result :: prev)
        })
        val sortedByName = splitByName.iterator.toSeq.sortBy(_._1)

        for ((name, resultList) <- sortedByName) {
            val iterations = resultList.length

            val sortedTimes = resultList.map({ _.timeNanosPerRequest }).toSeq.sorted

            /* First find the median */
            val (firstHalf, secondHalf) = sortedTimes splitAt (iterations / 2)
            val median = if (iterations % 2 == 0) {
                (firstHalf.last + secondHalf.head) / 2
            } else {
                secondHalf.head
            }

            /* Second find a trimmed mean */
            val toTrim = iterations / 8
            val trimmedTimes = sortedTimes.drop(toTrim).dropRight(toTrim)
            val numberAfterTrim = sortedTimes.length - toTrim * 2
            require(trimmedTimes.length == numberAfterTrim)
            val totalTrimmedTimes = trimmedTimes.reduceLeft(_ + _)

            val untrimmedAverageTime = sortedTimes.reduceLeft(_ + _).toDouble / iterations
            val trimmedAverageTime = totalTrimmedTimes.toDouble / numberAfterTrim

            val untrimmedMillisPerIteration = untrimmedAverageTime / TimeUnit.MILLISECONDS.toNanos(1)
            val trimmedMillisPerIteration = trimmedAverageTime / TimeUnit.MILLISECONDS.toNanos(1)
            val medianMillisPerIteration = median.toDouble / TimeUnit.MILLISECONDS.toNanos(1)

            printf("  %-35s %15.3f ms/request trimmed avg (%.3f median %.3f untrimmed) over %d iterations\n",
                name, trimmedMillisPerIteration, medianMillisPerIteration, untrimmedMillisPerIteration, iterations)
        }
    }
}

class MongoBenchmarker {
    private case class Collections[T](small : T, large : T)

    private final val NUMBER_IN_LARGE = 200

    private def createCollections[T](benchmark : MongoBenchmark[T]) : Collections[T] = {
        // create a collection of one small object
        val small = benchmark.cleanName + ".small"
        val smallHandle =
            benchmark.createCollection(small, Iterator(Map("foo" -> 10, "bar" -> "hello", "baz" -> 3.0)))

        // a large collection with large objects in it
        val large = benchmark.cleanName + ".large"
        val objects = for (i <- 1 to NUMBER_IN_LARGE) yield {
            val builder = Map.newBuilder[String, Any]
            for (j <- 1 to 200) {
                val s : String = "".padTo(j, j.toString.charAt(0))
                builder += Pair(j.toString, s)
            }
            // add recursive objects, just for fun
            builder += Pair("foo", Map("foo" -> 10, "bar" -> "hello", "baz" -> 3.0))
            builder += Pair("bar", Map("foo" -> 10, "bar" -> "hello", "baz" -> 3.0))
            builder += Pair("baz", Map("foo" -> 10, "bar" -> "hello", "baz" -> 3.0))
            builder.result
        }
        val largeHandle =
            benchmark.createCollection(large, objects)

        Collections(smallHandle, largeHandle)
    }

    private def prepOne[T](benchmarker : Benchmarker, b : MongoBenchmark[T]) : Unit = {
        printf("Prepping benchmark for %s\n", b.name)
        printf("  Connecting to database\n")
        b.openDatabase("localhost", 27017, "benchmark" + b.cleanName)

        printf("  Inserting data\n")
        val collections = createCollections(b)

        benchmarker.addBenchmark(8000, 1, "one small       (%s)".format(b.name), {
            b.findOne(collections.small)
        })

        def findOneHundred(b : MongoBenchmark[T], collection : T) : Unit = {
            val lazies = for (_ <- 1 to 100) yield {
                b.findOneAsync(collection)
            }
            // toList to be sure we invoke all findOneAsync before we start waiting
            for (f <- lazies.toList) {
                f.await()
            }
        }

        benchmarker.addBenchmark(1000, 100, "one small async (%s)".format(b.name), {
            findOneHundred(b, collections.small)
        })

        benchmarker.addBenchmark(1000, 1, "one large       (%s)".format(b.name), {
            b.findOne(collections.large)
        })

        benchmarker.addBenchmark(100, 100, "one large async (%s)".format(b.name), {
            findOneHundred(b, collections.large)
        })
        /*
        benchmarker.addBenchmark(50, 1, "all large       (%s)".format(b.name), {
            b.findAll(collections.large, NUMBER_IN_LARGE)
        })

        benchmarker.addBenchmark(10, 20, "all large async    (%s)".format(b.name), {
            val lazies = for (_ <- 1 to 20) yield {
                b.findAllAsync(collections.large, NUMBER_IN_LARGE)
            }
            for (f <- lazies.toList) {
                f.await()
            }
        })
        */
    }

    private def cleanupOne[T](b : MongoBenchmark[T]) : Unit = {
        printf("  cleaning up database for %s\n", b.name)
        b.cleanupDatabase()
        printf("  done with benchmark %s\n", b.name)
    }

    def run(benchmarks : TraversableOnce[MongoBenchmark[_]]) : Unit = {
        val benchmarker = new Benchmarker
        for (b <- benchmarks) {
            prepOne(benchmarker, b)
        }

        printf("Running benchmarks\n")
        benchmarker.runAll()

        printf("Cleaning up\n")
        for (b <- benchmarks) {
            cleanupOne(b)
        }

        printf("Results:\n")
        benchmarker.output()
    }
}
