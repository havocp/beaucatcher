
import akka.dispatch._
import akka.actor._
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch

object Main extends App {
    def waitABit(latch: CountDownLatch) = new Runnable() {
        override def run() = {
            Future.blocking()
            Thread.sleep(100)
            // use some CPU somehow
            /* var j = 1
            for (i <- 1 to 3000)
                j = (j * i * 1.8).toInt */
            latch.countDown()
        }
    }
    val unboundedPool = Executors.newFixedThreadPool(1000)
    val system = ActorSystem("Foo")

    val numWaits = 200

    // warm up threads
    for (i <- 1 to numWaits) {
        unboundedPool.execute(waitABit(new CountDownLatch(1)))
    }

    val unboundedLatch = new CountDownLatch(numWaits)
    val startUnbounded = System.currentTimeMillis()
    for (i <- 1 to numWaits) {
        unboundedPool.execute(waitABit(unboundedLatch))
    }
    unboundedLatch.await()
    val endUnbounded = System.currentTimeMillis()

    val akkaDefaultLatch = new CountDownLatch(numWaits)
    val startAkkaDefault = System.currentTimeMillis()
    for (i <- 1 to numWaits) {
        system.dispatcher.execute(waitABit(akkaDefaultLatch))
    }
    akkaDefaultLatch.await()
    val endAkkaDefault = System.currentTimeMillis()

    println("unbounded pool waited " + (endUnbounded - startUnbounded))
    println("akka default fork-join waited " + (endAkkaDefault - startAkkaDefault))

    System.exit(0)
}
