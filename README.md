Beaucatcher is a Scala API for accessing MongoDB, exploring
several ideas for taking better advantage of Scala.

If they work out, many of the ideas in Beaucatcher could be cool
as part of [Casbah](https://github.com/mongodb/casbah),
[Hammersmith](https://github.com/bwmcadams/hammersmith), or
[Salat](https://github.com/novus/salat) and I'd love to see that
or help accomplish it.

If you are interested in the ideas shown here then you are welcome
to send suggestions, send patches, steal ideas into official
drivers, or whatever you like. I'm friendly to all that; please
ask me for help.

Beaucatcher is not truly finished and "productized", see the file
[TODO](https://github.com/havocp/beaucatcher/blob/master/TODO),
but it may be close enough that you can enjoy using it if you
approach it in a collaborative spirit and aren't afraid of source
code.  Nothing in
[TODO](https://github.com/havocp/beaucatcher/blob/master/TODO) is
rocket science as far as I know but there are lots of little
things.

## What is it?

Beaucatcher has two TL;DR big-picture attributes:

 - it fits into the Scala/Akka world (e.g. async IO using Future,
   immutable data structures)
 - it's a small core with pluggable components factored out, to
   enable customization and experimentation

The core does not have a hard dependency on these factored-out
components:

 - the **network backend** (currently choice of Netty 3.3 or 3.4;
   _but could be_ raw NIO, Finagle, whatever)
 - the **protocol driver** (currently choice of mongo-java-driver
   or a custom driver that lets you plug the network backend; _but
   could be_ Hammersmith or a mock or whatever)
 - **query representation** (currently choice of Iterator, Map, or
   an lift-json-like abstract syntax tree called BObject; _but
   could be_ a DSL for example)
 - **document representation** (currently choice of Iterator, Map,
   your case class via reflection, or BObject; _but could be_ your
   own custom class or direct to a JSON string or whatever)

Most of this git repository can be thought of as _examples_
only. The core jars (`beaucatcher-base`, `beaucatcher-driver`,
`beaucatcher-mongo`) do not depend on Netty or mongo-java-driver
or scalap or any of that; those dependencies are _only if you want
them_. You can swap in your own approach instead and you'll be on
a level playing field with the included examples.

You can use Beaucatcher as a convenience wrapper around
[Mongo Java Driver](https://github.com/mongodb/mongo-java-driver),
or you can use it as a testbed to experiment with network
backends, or you can design your own query DSL or your own
[Salat](https://github.com/novus/salat)-style document mapper.

Diagram of the dependency graph: <a
href="http://github.com/havocp/beaucatcher/raw/master/Dependencies.png"><img
src="http://github.com/havocp/beaucatcher/raw/master/Dependencies.png"
alt="Dependency graph"/></a>

There's some example code in
[the examples directory](https://github.com/havocp/beaucatcher/blob/master/examples/).

## Ideas in Beaucatcher

1. Rather than a hardcoded `DBObject` type, Beaucatcher uses
typeclasses (as in Hammersmith). This means that **any type, such as
a JSON string or your own JSON representation or your own custom
domain object, can be encoded or decoded directly to MongoDB**.

2. Beaucatcher goes a step further than Hammersmith and has
**separate typeclasses for different contexts, such as queries and
query results**. This is some extra type-safety and cleanliness
(and if you invent your own query DSL, you don't have to implement
_decoding_ from MongoDB, just _encoding_).

3. Query and query-result **typeclasses can go to/from either the
wire format _or an iterator_**, which means you can create a
custom encoder or decoder without touching the wire protocol
directly.  This makes a custom encoder or decoder much easier to
write, and therefore more practical for app developers.  It also
means that Beaucatcher can efficiently use mongo-java-driver or a
mock as a backend, without going through a BSON serialization.

4. It provides **immutable** versions of basic BSON types such as
ObjectId, and **avoids mutable state throughout** (other than a
couple optimization cheats...).

5. Uses the `Future` type from Akka (which will be
scala.concurrent.Future in Scala 2.10) to provide **asynchronous
API**.

6. **For each call** to MongoDB, it's easy to choose either **sync
or async** API, and choose the **type of object you want to get
back** for results (e.g. a Map or JSON or a custom domain
object). For example, maybe in your Scala code you want to get a
typesafe object but for your REST APIs you want to decode from
Mongo directly into JSON. You can use the appropriate result type
for each. The syntax is like
`MyCollection.sync[ResultType].find()`.

7. Makes it easy to create a **singleton object representing a
collection, without keeping a reference to sockets or threads in
the singleton**. That is, there's a type (called
`CollectionAccess`) that knows how to set up indexes on a
collection and make queries to the collection; and a separate type
(called `Context`) which represents an actual database
connection. You import an implicit Context, then use the singleton
`CollectionAccess` to access the collection. This lets you work
with multiple databases and helps avoid memory leak problems
(singletons that reference threads and sockets cause GC
trouble). `CollectionAccess` ensures that indexes are set up a
single time for each database connection. If you're working with
multiple database connections, this can be very helpful. You can
also drop any global inititialization code you may have had to
ensure indexes on your collections. `CollectionAccess` with its
separate `Context` removes initialization boilerplate and reduces
global state.

8. **Selects the backend driver at runtime, via configuration
file** (using the
[config lib](https://github.com/typesafehub/config) which is also
slated for Scala 2.10). Currently the backends are
mongo-java-driver, or a thing called "channel driver" which has
pluggable network backends. At the moment the pluggable backends
include one for netty 3.3 and another for netty 3.4. But it would
be **easy to drop in and experiment with a non-netty nio backend
or a Finagle backend or whatever you like**. You could also drop
in a "mock" driver here and select it via configuration, a little
cleaner than the approach
[Fongo](https://github.com/foursquare/fongo) has to take with
mongo-java-driver.

9. Provides a completely optional **immutable BSON/JSON
representation** called `BObject`, which is a more Scala-native
alternative to `DBObject` and also works as a JSON library. Thanks
to typeclasses `BObject` is on _top_ of the dependency chain, not
at the bottom, though -- so if you don't like it, you can still
use all the Mongo stuff and create your own encoders/decoders for
whatever data type or query DSL you want to use.

10. Provides a very simple way to **decode BSON data into case
classes**; again, this is on top of the dependency chain and not on
the bottom, so it's completely optional. A more powerful solution
such as Salat could be used instead.

11. **Less shared state**; there's no concept of registering type
conversion handlers or setting collection- or database-wide
options. In turn there's no need to worry about initialization or
order of object creation.

12. (not yet implemented) **write concerns (get last error
parameters) should have defaults defined in configuration
per-collection**, so you won't have to modify your code to change
these.

So in short it's much more flexible if you want to replace or
customize any part of the MongoDB stack: query DSL, networking
layer, document encode/decode. And it has the Scala goodies like
async with the standardized-in-2.10 `Future`, immutable data
structures, and the powerful
[config lib](https://github.com/typesafehub/config).
Customization is always via typeclasses or configuration, not by
setting some kind of shared mutable state.

As a bonus there are a few features in here to reduce app
boilerplate and "setup" code, mostly the `CollectionAccess`
vs. `Context` split which gives you separate "schema" and
"connection" objects.

My hope is that the Beaucatcher design makes it easy to experiment
with different query DSLs, document representations,
networking/async-IO mechanisms, and so forth; you can experiment
without having to reimplement the entire MongoDB stack. It's easy
to do an isolated, apples-to-apples performance comparison of
these individual elements, as well.

## Code overview

Some pointers for understanding the module divisions and reading
the code...

 - a "channel" is responsible for handling a single TCP socket
   connected to mongod and just pulling raw requests on and off
   the wire; it implements
   [MongoSocket](https://github.com/havocp/beaucatcher/blob/master/channel/src/main/scala/org/beaucatcher/channel/MongoSocket.scala). Channels
   could be implemented with netty, Finagle, NIO, Spray, etc. (your network thingy of
   choice). Right now there are Netty 3.3 and 3.4 channels, with
   [identical code](https://github.com/havocp/beaucatcher/blob/master/channel-netty/src/main/scala/org/beaucatcher/channel/netty/NettyMongoSocket.scala).

 - a "driver" takes care of replica sets, pooling, etc. and
   provides Collection and Context objects that only have the
   MongoDB "primitive" operations. See the
   [driver-level collection interface](https://github.com/havocp/beaucatcher/blob/master/driver/src/main/scala/org/beaucatcher/driver/DriverCollection.scala)
   for example. Then the
   [implementation using mongo-java-driver](https://github.com/havocp/beaucatcher/blob/master/jdriver/src/main/scala/org/beaucatcher/mongo/jdriver/JavaDriverCollection.scala)
   and one
   [using channels](https://github.com/havocp/beaucatcher/blob/master/channel-driver/src/main/scala/org/beaucatcher/mongo/cdriver/ChannelDriverAsyncCollection.scala).

 - the "core" consists of three parts:
   - [beaucatcher-driver](https://github.com/havocp/beaucatcher/tree/master/driver) is the driver interface.
   - [beaucatcher-base](https://github.com/havocp/beaucatcher/tree/master/base) is a bunch of data types (BSON object ID,
     timestamp, command result, MongoException), and the codec
     (encoder/decoder) typeclasses. Nothing related to networking.
   - [beaucatcher-mongo](https://github.com/havocp/beaucatcher/tree/master/mongo)
     is the application-facing API for using Mongo collections.

 - beaucatcher-mongo loads a configured driver via reflection and
   provides "high level" operations on top of the driver: a bunch
   of convenience methods that call the driver's MongoDB
   primitives with different options, etc.  This is the
   application-visible API, see for example
   [AsyncCollection](https://github.com/havocp/beaucatcher/blob/master/mongo/src/main/scala/org/beaucatcher/mongo/AsyncCollection.scala).

 - The core includes encoders/decoders for Iterator[(String,Any)]
   and Map[String,Any] since those are pretty generic and
   agnostic ways to express queries and results.

 - above the core, the "bobject" and "caseclass" packages contain
   a couple of extra possible representations for queries and
   results. These are totally optional.

 - mandatory external dependencies are
   akka.dispatch.Future/scala.concurrent.Future and
   com.typesafe.config.Config. Both of these are slated to be
   included in Scala 2.10, so for 2.10 there would be zero
   external dependencies for the core. External dependencies would
   be only those needed for the selected channel/driver/codecs.
