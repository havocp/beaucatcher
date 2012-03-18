package org.beaucatcher.mongo

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

private[beaucatcher] final class Migrator {
    private val map = new ConcurrentHashMap[String, java.lang.Boolean](16, /* initial capacity - this is default */
        0.75f, /* load factor - this is the default */
        1) /* concurrency level - this is 1 writer, vs. the default of 16 */
    private val creationLock = new ReentrantLock

    // note: we EXPECT to be called recursively because the
    // migrate() function will have to in turn get some
    // Collection objects to run the migration. So with
    // migrate() on the stack we want to not-migrate but
    // just return.
    // With migrate() in another thread, we want to block
    // and wait for it.
    // To accomplish this we use a recursive lock so all other
    // threads block, and we only call migrate() on the outermost
    // lock.
    def ensureMigrated(context : Context,
        collectionName : String,
        migrate : (Context) => Unit) : Unit = {
        // we're trying to avoid locking if we've
        // already done the migrate()
        val b = map.get(collectionName)
        if (b eq null) {
            creationLock.lock()
            try {
                val beatenToIt = map.get(collectionName)
                if (beatenToIt eq null) {
                    val depth = creationLock.getHoldCount()
                    require(depth > 0)
                    if (depth == 1) {
                        migrate(context)
                        map.put(collectionName, java.lang.Boolean.TRUE)
                    }
                }
            } finally {
                creationLock.unlock()
            }
        }
    }
}
