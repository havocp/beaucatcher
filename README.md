Beaucatcher is a Scala library for JSON parsing/rendering _and_ for
using MongoDB collections.

It wraps and builds on a "driver library" (select Casbah or
Hammersmith by mixing in the trait for the one you want).

Please see http://beaucatcher.org/ for a high-level description,
rationale, list of what works and what doesn't, and all that.

The rest of this README is more about introducing the specifics of the
library - it's the getting started guide, until there's a real getting
started guide.

You may want to get the background at http://beaucatcher.org/ before
proceeding.

## Overview

The core of Beaucatcher is to make it easier to get data in a
convenient format and massage the data a bit on its way in or out of
MongoDB. Conceptually you can think of Beaucatcher as a pipeline from
a BSON AST tree coming from MongoDB, which is then converted to either
a case class (to work with in your Scala code) or to JSON (to be
shipped to an HTTP client, typically). On a per-call basis, you can
make the choice of desired datatype (BSON, JSON, case class) for
results, and choose synchronous or asynchronous for the call.

(You can use any custom class, not just a case class, by implementing the
conversion to/from BSON for that class.)

(In principle, though not yet, Beaucatcher could convert directly from
the MongoDB wire protocol to your desired format and thus be extra
fast.)

A goal is to support typical web application needs (for example, a
JSON HTTP service may need CRUD operations on a MongoDB collection,
with some validation or filtering of the data).

The Beaucatcher public API is not very large. It's split into a bunch
of jars to let people pick-and-choose dependencies. But there are
essentially these things:

 - an immutable AST tree for BSON and JSON (similar to lift-json, but
   without a lot of API for transformations or other goodies, it's just
   basic). BSON and JSON are handled in a uniform way, so converting
   from one to the other is fast and there aren't two APIs to learn.
 - a JSON parser and renderer.
 - sync and async "DAO" traits, that have find() and all those methods
   with their accompanying options.
 - a `CollectionOperations` trait that's essentially a set of DAOs,
   matrix of (result-type x asyncness). The idea is that for a
   collection, you define a case class with its schema, and have its
   companion object extend `CollectionOperationsWithCaseClass`
 - a `JsonMethods` trait that implements CRUD operations based on
   JSON strings for the collection; you might hook this up to your
   incoming HTTP requests. You could have a companion object extend
   this too. JSON can be validated against your case class.

You can probably get some sense of the library by looking at the unit
tests. (Though they have an awkward abstraction to share code between
the Casbah and Hammersmith projects, making them look a little more
cluttered than your app will.)

Beaucatcher builds on Casbah or Hammersmith, and only implements
collection operations (find, save, remove, etc.). You still need to
use those libraries directly to operate on the MongoDB connection, and
Beaucatcher is not a "MongoDB driver" (i.e. it doesn't implement any
wire protocol stuff, it's a layer on top).

## BSON/JSON tree

 - Beaucatcher represents both BSON and JSON as a tree of immutable case class
   nodes (as in lift-json).
 - a `BValue` is a tree of BSON nodes and a `JValue` is a tree of JSON
   nodes. A `JValue` is-a `BValue`. `BValue` types include `BObject`,
   `BDouble`, `BString`, `BArray`, `BObjectId` etc. The primitives
   such as `BString` are also `JValue`, but there are `JObject` and
   `JArray` containers separate from `BObject` and `BArray`. BSON
   types such as `BObjectId` do not extend the `JValue` trait since
   they don't appear in plain JSON.

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

The numeric types implement `ScalaNumericConversions` which
means you can call `isWhole`, `isValidInt`, `intValue`, etc. on them.
Also it means `BInt32(42) == 42` for example.

Unlike in lift-json, there's no `JField` type, because I don't think
`JField` is really a `JValue` - you can't use it in most places a
`JValue` is expected. So that's "fixed" (I don't know if everyone
considers it a bug, but it seemed weird to me).

You can convert a `BValue` (or `JValue`) to plain Scala values using `unwrapped`:

    val scalaMap = bobject.unwrapped

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

There's also an XPath-inspired `select()` method for BSON and JSON:

    bobject.select("foo/bar")
    bobject.select(".//bar")
    bobject.select("foo/*")
    bobject.selectAs[Int](".//bar")

See the API documentation for more details on the syntax.

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

 - sometimes you want a BSON or JSON tree to manipulate, sometimes you
   want a JSON string, and sometimes you want a typesafe class instead
   of a *SON blob.
 - the basic data access interface (see SyncDAO.scala) has type
   parameters for the query, entity, and id types.
 - typically you would use a pipeline of DAO objects, where the DAO
   that gives you back case classes builds on the DAO that gives
   you back `BObject`. See CollectionOperations.scala for a trait that
   gives you the DAO pipeline.
 - you can then choose to query for either a raw BSON tree or
   the case class, and write generic queries that support both.
 - you can override and customize the BSON-to-case-class conversion
 - it's easy to convert `BObject` to JSON, a case class, or a plain
   Java or Scala map.
 - you could use the JSON for your web APIs, and in templates you could
   use the plain maps or the case class. In code, the case class
   might be most convenient.

Data access starts with an abstract trait that defines an interface
with `find()`, `insert()`, `remove()`, etc.:

    abstract trait SyncDAO[QueryType, EntityType, IdType]

There are subtypes of the trait for `BObject` and case class entity
types.

Then there's a `CollectionOperations` trait, with some subclasses that
connect to Casbah. This trait sets up both a `BObject` and a custom
class DAO. You might use it like this:

    trait MyAppMongoProvider
       extends MongoConfigProvider
       // note here you select Casbah vs. Hammersmith
       with CasbahBackendProvider {
       override val mongoConfig = new SimpleMongoConfig("mydbname", "localhost", 27017)
    }

    package foo {
        case class Foo(_id : ObjectId, intField : Int, stringField : String)

        object Foo
            extends CollectionOperationsWithCaseClass[Foo, ObjectId]
            with MyAppMongoProvider {
            def customQuery[E](implicit chooser : SyncDAOChooser[E, _]) = {
                syncDAO[E].find(BObject("intField" -> 23))
            }
        }
    }

Notice the `syncDAO[E]` value, where `E` would be either the
`Foo` case class, or `BObject`. If the call you're making doesn't
return objects, you can omit the type parameter and just write:

    Foo.syncDAO.count()

or whatever.

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

## Custom class rather than case class

If you'd rather use an arbitrary custom class instead of a case class
with a Mongo collection, then extend `CollectionOperations` rather
than `CollectionOperationsWithCaseClass` and implement the field
`entityBObjectEntityComposer` which must be a "composer" object that
has two methods, `entityIn` to go from your entity object to
`BObject`, and `entityOut` to go the other way.

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

To be clear, Beaucatcher does not contain any HTTP code; you'd have to
write some trivial glue between HTTP in your web stack of choice, and
the `JsonMethods` trait.

The methods in `JsonMethods` generally take the trailing part of the
URL (which would be the object ID) and then take and/or return a JSON
string representing the object.

## Queries

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
the API, so Beaucatcher just uses the native MongoDB approach just as
MongoDB would send it on the wire.

If you aren't sure how to do something, often the unit tests will have
an example.

## Final note!

Beaucatcher is not mature code and if it breaks you get the pieces
(though the test suite is pretty decent, fwiw).  I am enjoying it so I
hope you'll at least find the ideas interesting.
