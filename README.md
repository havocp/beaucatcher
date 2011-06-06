Beaucatcher is a library for using MongoDB collections from Scala. It
wraps and builds on a "driver library" (currently Casbah, working on
Hammersmith).

("Beaucatcher" is also the name of the mountain in the middle of
Asheville, NC, if you're wondering where the word comes from.)

Note: this project isn't quite ready to use, unless you're the kind
that's happy to hack on the library itself. You're welcome to send
comments or play around with it. If you have any trouble let me know.

## Overview

The core of Beaucatcher is to make it easier to get data in a
convenient format and massage the data a bit on its way in or out of
MongoDB. Conceptually you can think of Beaucatcher as a pipeline from
a BSON AST tree coming from MongoDB, which is then converted to either
a case class (to work with in your Scala code) or to JSON (to be
shipped to an HTTP client, typically). On a per-call basis, you can
make the choice of desired datatype (BSON, JSON, case class) for
results, and choose synchronous or asynchronous for the call.

A goal is to support typical web application needs (for example, a
JSON HTTP service may need CRUD operations on a MongoDB collection,
with some validation or filtering of the data).

The Beaucatcher public API is not very large. It's split into a bunch
of jars to let people pick-and-choose dependencies. But there are
essentially these things:

 - an immutable AST tree for BSON and JSON (similar to lift-json, but
   without a lot of API for transformations or other goodies; and just
   uses lift-json for parsing/stringification for now)
 - sync and async "DAO" traits, that have find() and all those methods
 - a `CollectionOperations` trait that's essentially a set of DAOs,
   matrix of (result-type x asyncness). The idea is that for a
   collection, you define a case class with its schema, and have its
   companion object extend `CollectionOperations`
 - a `JsonMethods` trait that implements CRUD operations based on
   JSON strings for the collection; you might hook this up to your
   incoming HTTP requests. You could have a companion object extend
   this too.

You can probably get some sense of the library by looking at the unit
tests.

## Credits

There are ideas and some code from Salat, BlueEyes persistence, and
lift-json included here. You could think of this library as taking the
MongoDB-object-to-case-class stuff from Salat, the
MongoDB-to-syntax-tree stuff from BlueEyes persistence, and pulling
them together in a stripped-down form. I never claimed to be original.

Beaucatcher builds on Casbah or Hammersmith, and only implements
collection operations (find, save, remove, etc.). You still need to
use those libraries directly to operate on the MongoDB connection or
db, and Beaucatcher is not a "MongoDB driver" (i.e. it doesn't
implement any wire protocol stuff, it's a layer on top).

## Status

The current status of the library is "in progress" - I'm actively
trying to make it more "productized" so other people can pick it up
and use it. Also, hopefully the boundaries between this library and
others such as Salat and Hammersmith will change over time, as
features are "pushed down" into those libraries when it makes sense.

Please don't expect a stable API for now (though if you snapshot the
code, which you are welcome to do, the API won't change on your
copy!).

See also the Limitations section below.

## More detailed overview

Some ideas in the library are:

 - sometimes you want a BSON or JSON tree to manipulate, sometimes you
   want a JSON string, and sometimes you want a typesafe class instead
   of a *SON blob. The library should support all of the above.
 - represents both BSON and JSON as a tree of immutable case class
   nodes (as in lift-json).
 - a `BValue` is a tree of BSON nodes and a `JValue` is a tree of JSON
   nodes. A `JValue` is-a `BValue`. `BValue` types include `BObject`,
   `BDouble`, `BString`, `BArray`, `BObjectId` etc. The primitives
   such as `BString` are also `JValue`, but there are `JObject` and
   `JArray` containers separate from `BObject` and `BArray`. BSON
   types such as `BObjectId` do not extend the `JValue` trait since
   they don't appear in plain JSON.
 - the basic data access interface (see SyncDAO.scala) has type
   parameters for the query, entity, and id types.
 - typically you would use a pipeline of DAO objects, where the DAO
   that gives you back case classes builds on the DAO that gives
   you back `BObject`. See CollectionOperations.scala for a trait that
   gives you the DAO pipeline. (CasbahCollectionOperations extends
   the trait with more concrete implementation.)
 - you can then choose to query for either a raw BSON tree or
   the case class, and write generic queries that support both.
 - you can override and customize the BSON-to-case-class conversion.
 - it's easy to convert `BObject` to JSON, a case class, or a plain
   Java or Scala map.
 - you could use the JSON for your web APIs, and in templates you could
   use the plain maps or the case class. In code, the case class
   might be most convenient.
 - the library is layered nicely so Casbah dependencies are separate
   from BSON-only dependencies, the case class stuff is separate from
   the `BObject` stuff, etc. You can pick-and-choose which pieces
   to use.
 - currently JSON parsing and generation are done with lift-json but
   it may be nicer to drop this dependency sometime. The dependency
   isn't in the API.
 - make it simple to code backbone.js-style REST CRUD methods on
   a MongoDB collection
 - since we have a nice schema in the form of the case class, support
   validating untrusted JSON against the case class

## BSON/JSON tree

This is similar to JValue in lift-json (see
https://github.com/lift/lift/blob/master/framework/lift-base/lift-json/README.md ).

You can build a BSON object:

    BObject("a" -> 1, "b" -> "foo")

or a JSON object:

    JObject("a" -> 1, "b" -> "foo")

You can convert a `BValue` to a `JValue` with `toJValue()`, this
converts all the BSON-specific types into plain JSON types.

In BSON or JSON, objects implement `Map` and arrays implement
`LinearSeq`.  So you can just use all the normal Scala APIs on
them. (Most of the methods in lift-json are _not_ included, replaced
by the usual Scala collection APIs.)

There are some implicit conversions in `com.ometer.bson.Implicits._` if
you'd like to use them.

The numeric types implement `ScalaNumericConversions` which basically
means you can call `isWhole`, `isValidInt`, `intValue`, etc. on them.
Also it means `BInt32(42) == 42` for example.

Unlike in lift-json, there's no `JField` type, because I don't think
`JField` is really a `JValue` - you can't use it in most places a
`JValue` is expected. So that's "fixed" (I don't know if everyone
considers it a bug, but it seemed weird to me).

You can convert a `BValue` (or `JValue`) to plain Scala values using `unwrapped`:

    val scalaMap = bobject.unwrapped

or to plain Java values (maybe handy for template languages that
aren't Scala-aware, or for Java APIs):

    val javaMap = bobject.unwrappedAsJava

And you can convert to JSON:

    val jsonString = bobject.toJson()

or parse it:

    val jobject = JValue.parseJson(jsonString)

JSON methods take an optional `JsonFlavor` argument, which is best
described here:
http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON

At the moment only the `JsonFlavor.CLEAN` (no type information) flavor
is really supported.

I find immutable trees a lot nicer to work with in Scala than the
Java-ish `DBObject` interface.

## Case class conversion

There's a `ClassAnalysis` class, based on Salat's `Grater`, but
chopped down and no longer mongodb-specific, so all it does is convert
a case class to and from a map. You can also iterate over field names
and values.

Unlike Salat:

 - there's no type conversion for fields, so fields have to match the
   types in the map. The one exception is support for `Option`; if a
   field is an `Option`, it can be missing from the map. (I do want
   to add safe numeric conversions, like putting Int in Long.)
 - there are no annotations, so you can't ignore fields or anything like that
 - there's no global hash lookup of graters, you would have to build
   that yourself or use the DAO stuff described below.
 - without the global hash, case class fields inside case classes can't
   really work, so you have to resolve "joins" by hand right now

Usage looks like:

    val analysis = new ClassAnalysis(classOf[IntAndString])
    val map = analysis.asMap(IntAndString(42, "brown fox"))
    assertEquals(Map("foo" -> 42, "bar" -> "brown fox"), map)

The `ClassAnalysis` needs to be cached somehow since it's expensive.
The DAO objects described next will do this for you.

## Data access

Data access starts with an abstract trait that defines an interface
with `find()`, `insert()`, `remove()`, etc.:

    abstract trait SyncDAO[QueryType, EntityType, IdType]

There are subtypes of the trait for `BObject` and case class entity
types.

Then there's a `CollectionOperations` trait, with some subclasses that
connect to Casbah. This trait sets up both a `BObject` and a case
class DAO. You might use it like this:

    package foo {
        case class Foo(_id : ObjectId, intField : Int, stringField : String)

        object Foo extends CasbahCollectionOperationsWithObjectId[Foo] {
            override protected lazy val collection : MongoCollection = {
                MongoUtil.collection("foo")
            }

            def customQuery[E : Manifest]() = {
                syncDAO[E].find(BObject("intField" -> 23))
            }
        }
    }

Notice the `syncDAO[E]` value, where `E` would be either the
`Foo` case class, or `BObject`. You can also directly refer to
`caseClassSyncDAO` or `bobjectSyncDAO` rather than dynamically
selecting based on the type parameter, if you want.

The purpose of `syncDAO[E]` is to let you write one query, and use it
to get back either a `BObject` or a case class depending on what
you're doing. You probably want a `BObject` in order to dump some JSON
out over HTTP, but a case class if you're going to write some code in
Scala to manipulate the object.

If you wanted to change the mapping from the `BObject` layer to the
case class layer, you can override the "composers" included in the
`CasbahCollectionOperations`. For example, there's a field
`caseClassBObjectEntityComposer : EntityComposer[EntityType, BObject]`
that converts between the case class and the `BObject`. The idea is
that you could do things such as rename fields in here, making it an
alternative to annotations for that.

## Parsing JSON validated against a case class

To go directly from JSON to BSON (to the `BValue` rather than `JValue`
type), some form of schema is needed to figure out types; for example,
to figure out that an `ObjectId`-formatted string is an `ObjectId` and not
a string.

The natural schema is the case class.

    val analysis = new ClassAnalysis(classOf[ObjectIdAndString])
    val bson = BValue.parseJson(jsonString, analysis)

This will:

 - validate the JSON (ensuring it has all fields in the case
class)
 - convert to BSON types to match the case class (for example,
a string becomes an ObjectId if the field in the case class is an
ObjectId)
 - remove any fields not found in the case class

After parsing the JSON with the case class as schema, building an
instance of the case class should work (if not, it's a bug, I would
think):

    val caseClassInstance = analysis.fromMap(bson.unwrapped)

There are several improvements that would be nice here: avoiding the
"unwrapping" overhead, adding a `BValue.toCaseClass` convenience
method.

## Auto-implementing REST-style CRUD operations with JSON

There's a trait called `JsonMethods` which implements a "backend" that
corresponds to backbone.js-style CRUD operations on a MongoDB
collection.

To be clear, this library does not contain any HTTP code; you'd have
to write some trivial glue between HTTP in your web stack of choice,
and the `JsonMethods` trait.

The methods in `JsonMethods` generally take the trailing part of the
URL (which would be the object ID) and then take and/or return a JSON
string representing the object.

## Limitations

At the moment this library doesn't do anything related to setting up
your database and collections, you have to use Casbah directly to
create indexes and things of that nature. There's no scanning of case
classes to figure out what your collections are as in Salat or JPA,
that's all up to you. The API here is just for using collections that
already exist.

There's no support for cursors. Most production apps will probably
need that.

Not really any docs other than this file you're reading, though there
isn't a lot of code anyhow, so I'm sure you can figure it out.

There's no DSL stuff for queries (as in Casbah) or for JSON
manipulation (as in lift-json).

If you want to sort or add hints or things like that you have to
manually build the appropriate query object, so for example if the
query is:

    dao.find(BObject("a" -> 42))

if you want to sort you have to do this for now:

    dao.find(BObject("query" -> BObject("a" -> 42),
                     "orderby" -> BObject("whatever" -> 1)))

Casbah does this with cursors instead (`find()` returns a cursor,
which you can call `sort()` on, which then modifies the query before
sending it off) but that approach seems tricky for an async flavor of
the API, so thinking of doing something else. Maybe taking cues from
whatever Hammersmith does if anything.

This is not mature code and if it breaks you get the pieces.  But I am
enjoying it so I hope you'll at least find the ideas interesting.
