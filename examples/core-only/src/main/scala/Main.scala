import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import akka.dispatch.Await
import akka.util.duration._

case class Foo(_id: ObjectId, anInt: Int, aString: String)

// This is an example that doesn't use the BObject or caseclass-reflection encoders/decoders,
// it's just all done by hand depending only on the core mongo jar
object Main extends App {

    // default context gets its config from reference.conf and application.conf
    implicit val context = Context()

    // remove any old documents
    Foo.sync.removeAll()

    // collection operations using Foo case class
    Foo.sync.insert(Foo(ObjectId(), 42, "Hello"))
    val found = Foo.sync.findOne(Iterator("aString" -> "Hello")).getOrElse(throw new Exception("Didn't find document"))

    println("Found document as Foo: " + found)

    // collection operations using Map
    Foo.sync[Map[String, Any]].insert(Map("_id" -> ObjectId(), "anInt" -> 43, "aString" -> "World"))
    val foundMap = Foo.sync[Map[String, Any]].findOne(Iterator("anInt" -> 43)).getOrElse(throw new Exception("Didn't find document"))

    println("Found document as Map: " + foundMap)

    // an async collection operation. Note that in Scala 2.10 this
    // will be part of the Scala library, not the separate Akka
    // package.
    val futureCount = Foo.async.count()
    val futureDocs = Foo.async.find()
    val latch = new java.util.concurrent.CountDownLatch(1)
    for {
        count <- futureCount
        docs <- futureDocs
    } {
        // this code runs asynchronously in another thread
        println("There are " + count + " documents in the collection '" + Foo.collectionName + "' in database '" + context.database.name + "'")
        println("The documents are: " + docs.blocking(1 second).toList)
        latch.countDown()
    }

    // Wait for the above to complete.
    // Blocking on a latch like this not recommended in real-life code, you'd
    // just use the sync API instead
    latch.await()

    context.close()

    System.exit(0)
}

/* We're doing this manually; if you use the bobject or caseclass packages, most of this is pre-built.
 * Also, using a CollectionAccess object like this is optional, you could also just manually juggle
 * collection objects as you would with mongo-java-driver.
 */
object Foo
    // Queries are Iterator[(String, Any)], the _id field is ObjectId, and we can access
    // the documents in the collection ("entities") as either a Map or a Foo.
    // Individual values (as returned by distinct() for example) are Any.
    extends CollectionAccessWithTwoEntityTypes[Iterator[(String, Any)], ObjectId, Foo, Any, Map[String, Any], Any] {

    // Note: You can override collectionName here, but by default
    // it's based on the object name.

    // This will be called once on each Context and needs to
    // synchronously create your collections and indexes
    override def migrate(implicit context: Context): Unit = {
        sync.ensureIndex(Iterator("aString" -> 1))
    }

    // the codec set with the first (default) entity type, Foo
    override def firstCodecSet: CollectionCodecSet[Iterator[(String, Any)], Foo, Foo, ObjectId, Any] = FooCodecSet

    // The codec set with the second entity type, Map[String, Any]
    override val secondCodecSet = CollectionCodecSetMap[ObjectId]()(IdEncoders.objectIdIdEncoder)

    // Codec sets can be built up through either composition or inheritance;
    // this is an example of using inheritance.
    // Note that the caseclass package automates this via reflection,
    // so we're only doing this by hand as an example, there isn't
    // much of a real reason to do it by hand.
    private object FooCodecSet
        // Codec set with Iterator[(String,Any)] queries,
        // Foo entities for both encode and decode (updates and results),
        // ObjectId for _id,
        // and Any for individual values e.g. from distinct()
        extends CollectionCodecSet[Iterator[(String, Any)], Foo, Foo, ObjectId, Any]
        with CollectionCodecSetEntityCodecsMapBased[Foo]
        with CollectionCodecSetQueryEncodersIterator
        with CollectionCodecSetValueDecoderAny[ErrorIfDecodedDocument]
        with CollectionCodecSetIdEncoderObjectId {

        // CollectionCodecSetValueDecoderAny needs to know what to do
        // if there's a document inside the decoded document;
        // in this case we just throw an exception by using ErrorIfDecodedDocument
        // which generates an error if it's decoded.
        override def nestedDocumentQueryResultDecoder = ErrorIfDecodedDocument.queryResultDecoder

        // CollectionCodecSetEntityCodecsMapBased requires us to implement to/from Map
        // there's also an IteratorBased or you can go directly to a byte buffer.
        override def fromMap(m: Map[String, Any]) = {
            val fooOption = for {
                _id <- m.get("_id").map(_.asInstanceOf[ObjectId])
                anInt <- m.get("anInt").map(_.asInstanceOf[Number].intValue)
                aString <- m.get("aString").map(_.asInstanceOf[String])
            } yield Foo(_id, anInt, aString)
            fooOption.getOrElse(throw new MongoException("Missing fields in Foo document"))
        }

        override def toMap(foo: Foo): Map[String, Any] = {
            Map("_id" -> foo._id, "anInt" -> foo.anInt, "aString" -> foo.aString)
        }
    }

}
