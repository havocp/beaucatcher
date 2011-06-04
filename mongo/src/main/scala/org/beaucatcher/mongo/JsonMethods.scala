package com.ometer.mongo

import com.ometer.bson.Implicits._
import com.ometer.bson._
import com.ometer.ClassAnalysis
import org.bson.types._

/**
 * The idea of this trait is to implement a backbone.js-style set of methods
 * for a MongoDB collection. backbone.js just seems like a good "spec" for
 * a set of CRUD methods in REST format. You can pretty easily adapt this
 * to map to HTTP differently, though.
 *
 * See http://documentcloud.github.com/backbone/#Sync
 *
 * My still-untested interpretation follows.
 *
 * CRUD maps to REST like this:
 *  create = POST   /collection
 *  read = GET   /collection[/id]
 *  update = PUT   /collection/id
 *  delete = DELETE   /collection/id
 *
 * Create and Update should return the modified fields (the fields different from those passed in)
 * though they are allowed to return unmodified fields, with an efficiency cost.
 *
 * Read should always return all fields of the object. Without the /id, it returns a JSON array of all
 * objects.
 *
 * Delete I guess should return an empty body, though I'm not sure what backbone.js expects.
 *
 * FIXME: This class probably needs some way to pass in application-specific data (such as
 * the currently-logged-in-user) to the various methods. I guess you could use thread local
 * but maybe there's something cleaner.
 */
trait JsonMethods[SchemaType <: Product] {
    /** Point this to a DAO to use to store BObject */
    protected def jsonDAO : SyncDAO[BObject, BObject, _]
    /** Since we're a trait, we don't have a manifest and you have to provide this */
    protected def jsonAnalysis : ClassAnalysis[SchemaType]
    /** If you want to override the JSON flavor, do so here */
    protected val jsonFlavor : JsonFlavor = JsonFlavor.CLEAN

    /**
     * This method creates the query that will be used to
     * find an existing object, based on the URL path segment
     * used in the HTTP request.
     *
     * By default it assumes the path segment is an ObjectId
     * stored in the "_id" field. (In the future, the default
     * may be smart and look at the type of the _id field
     * in the schema case class.)
     *
     * To change that, override here.
     */
    protected def createQueryForObject(path : String) : BObject = {
        BObject("_id" -> BObjectId(new ObjectId(path)))
    }

    /**
     * This method creates the query that will be used to list
     * all of the objects in the collection. By default this
     * isn't implemented, because it's unlikely anyone wants
     * an API that returns an entire unfiltered MongoDB collection.
     * More likely, you want to filter by user or something, which
     * you can do by overriding this. This is abstract to be sure
     * you consider the issues.
     */
    protected def createQueryForAllObjects() : BObject

    /**
     * This function should convert a trailing path component to the JValue representation
     * of the object ID, or throw a JsonValidationException if the path is not a well-formed ID value.
     * By default, this validates an ObjectId and converts it to a BString, but you can override
     * if your object's ID is not an ObjectId.
     *
     * (In the future, the default
     * may be smart and look at the type of the _id field
     * in the schema case class.)
     */
    protected def parseJValueIdFromPath(path : String) : JValue = {
        try {
            new ObjectId(path)
        } catch {
            case _ => throw new JsonValidationException("not a valid id: " + path)
        }
        BString(path)
    }

    /**
     * When creating an object, this function generates a new ID for it. By default,
     * it creates a new ObjectId as a BString. Override this if your ID type is
     * not ObjectId.
     */
    protected def generateJValueId() : JValue = {
        BString(new ObjectId().toString)
    }

    /**
     * Override this if you want to modify JSON that's coming in. You can
     * also throw a JsonValidationException from here. This method will
     * be called before validating the JSON against the case class schema.
     * This method must return an object, not any other kind of JValue.
     *
     * By default this method forces the JValue to be a JObject and
     * adds an "_id" field if there wasn't one before. Also by default
     * if there's a path passed in, it's used for the _id and must match
     * any existing _id. This method's default implementation invokes
     * parseJValueIdFromPath() and generateJValueId(); usually you would
     * override those to modify your ID type.
     *
     * In this method, you could also "fix up" JSON (for example add default values for missing fields)
     * before the JSON gets validated.
     *
     * This method is called for creating and for updating.
     */
    protected def modifyIncomingJson(path : Option[String], jvalue : JValue) : JObject = {
        jvalue match {
            case jobject : JObject => {
                if (jobject.contains("_id")) {
                    if (path.isDefined) {
                        val idInObject : JValue = jobject.get("_id").get
                        if (parseJValueIdFromPath(path.get) != idInObject)
                            throw new JsonValidationException("Posted JSON containing _id %s to path %s".format(idInObject, path.get))
                    }
                    jobject
                } else {
                    val id : JValue = {
                        if (path.isDefined)
                            parseJValueIdFromPath(path.get)
                        else
                            generateJValueId()
                    }

                    jobject + ("_id", id)
                }
            }
            case _ => throw new JsonValidationException("JSON value must be an object i.e. enclosed in {}")
        }
    }

    /**
     * Override this if you want to modify the BSON after it's
     * validated against the case class schema and converted from
     * JSON. You can also throw a JsonValidationException from here.
     * So you can use this to do additional validation, or to add or remove
     * or rename fields before storing in the database. This is called for
     * creating and for updating.
     */
    protected def modifyIncomingBson(bobject : BObject) : BObject = {
        bobject
    }

    /**
     * Override this to modify BSON on its way out. Called when reading
     * and when returning an object from create and update.
     */
    protected def modifyOutgoingBson(bobject : BObject) : BObject = {
        bobject
    }

    /**
     * Override this to modify JSON on its way out. Called when reading
     * and when returning an object from create and update.
     */
    protected def modifyOutgoingJson(jobject : JObject) : JObject = {
        jobject
    }

    // FIXME we want some way to return only changed fields, at least
    // to the extent we can do that without doing another query and being
    // slow.
    private def outgoingString(bobject : BObject) : String = {
        modifyOutgoingJson(modifyOutgoingBson(bobject).toJValue(jsonFlavor)).toJson()
    }

    /**
     * This function is intended to implement HTTP POST to
     * a URL like "/collection", creating a new object. It returns
     * a JSON string with the new object, mostly so you can see the
     * newly-created ID for the object.
     * See updateJson for most other details.
     */
    def createJson(json : String) : String = {
        val bobject = parseJson(None, json)
        jsonDAO.save(bobject)
        // FIXME here in theory we only have to return the new ID
        outgoingString(bobject)
    }

    /**
     * This function is intended to implement HTTP PUT (updating an object).
     * The provided path should be the last segment of the path, or whatever part
     * identifies the object. The JSON will be validated and then stored in the
     * database.
     *
     * By default the assumption is that "path" is an ObjectId string which
     * corresponds to the _id field in the object.
     * You can modify the pipeline from JSON to BSON, including the path-to-ID mapping,
     * by overriding the modifyIncomingJson() or modifyIncomingBson() hooks.
     *
     * The new object is returned back as a JSON string.
     * The returned-back new object is modified by
     * the same methods that affect getJson()'s returned object.
     */
    def updateJson(path : String, json : String) : String = {
        val bobject = parseJson(Some(path), json)
        jsonDAO.update(createQueryForObject(path), bobject)
        // FIXME here in theory we only have to return the changed fields
        outgoingString(bobject)
    }

    /**
     * This function is intended to implement HTTP GET, retrieving an object
     * or list of objects.
     *
     * By default the assumption is that "path" is an ObjectId string which corresponds
     * to the _id field in the object. You can change this assumption by overriding
     * the pipeline from BSON to JSON, i.e. the modifyOutgoingBson() and modifyOutgoingJson()
     * hooks.
     *
     * If no path is provided, then this reads all objects as defined by createQueryForAllObjects().
     */
    def readJson(path : Option[String]) : Option[String] = {
        if (path.isDefined) {
            // GET a single ID
            jsonDAO.findOne(createQueryForObject(path.get)) match {
                case Some(bobject) =>
                    Some(outgoingString(bobject))
                case _ =>
                    None
            }
        } else {
            // GET all objects
            val all = jsonDAO.find(createQueryForAllObjects())
            val b = JArray.newBuilder
            for (o <- all) {
                b += modifyOutgoingJson(modifyOutgoingBson(o).toJValue(jsonFlavor))
            }
            Some(b.result.toJson())
        }
    }

    /**
     * Intended to implement HTTP DELETE, deletes the object identified by the path.
     */
    def deleteJson(path : String) : Unit = {
        jsonDAO.remove(createQueryForObject(path))
    }

    private def fromJValue(path : Option[String], jvalue : JValue) : BObject = {
        val jobject : JObject = modifyIncomingJson(path, jvalue)
        BValue.fromJValue(jobject, jsonAnalysis, jsonFlavor) match {
            case bobject : BObject =>
                modifyIncomingBson(bobject)
            case wtf =>
                throw new JsonValidationException("JSON must be an object \"{...}\" not " + wtf.getClass.getName)
        }
    }

    private def parseJson(path : Option[String], json : String) : BObject = {
        fromJValue(path, JValue.parseJson(json))
    }

    /**
     * Parses JSON against the schema, calling modify hooks in the same way as createJson(), i.e.
     * the parsed JSON ends up as it would normally be stored in MongoDB.
     */
    def parseJson(json : String) : BObject = {
        parseJson(None, json)
    }

    /**
     * Parses JSON containing an array of objects, validating each one against the
     * schema. The parsed objects end up as they would normally be stored in MongoDB
     * by createJson() or updateJson()
     */
    def parseJsonArray(json : String) : BArray = {
        val jvalue = JValue.parseJson(json)
        jvalue match {
            case jarray : JArray =>
                val b = BArray.newBuilder
                for (o <- jarray) {
                    b += fromJValue(None, o)
                }
                b.result
            case wtf =>
                throw new JsonValidationException("JSON must be an array of objects \"[...]\" not " + wtf.getClass.getName)
        }
    }
}
