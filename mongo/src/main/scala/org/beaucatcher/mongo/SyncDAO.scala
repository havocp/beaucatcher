package org.beaucatcher.mongo

import com.mongodb.WriteResult
import org.beaucatcher.bson._

sealed trait Fields {
    val included : Set[String]
    val excluded : Set[String]

    override def toString = "Fields(included=%s,excluded=%s)".format(included, excluded)
}

sealed trait IncludeIdFlag
case object WithId extends IncludeIdFlag
case object WithoutId extends IncludeIdFlag

object AllFields extends Fields {
    override val included : Set[String] = Set.empty
    override val excluded : Set[String] = Set.empty

    override def toString = "AllFields"
}

class IncludedFields(includeId : IncludeIdFlag, override val included : Set[String]) extends Fields {
    override val excluded : Set[String] = if (includeId == WithId) Set.empty else Set("_id")
}

object IncludedFields {
    def apply(included : String*) = new IncludedFields(WithId, included.toSet)
    def apply(includeId : IncludeIdFlag, included : String*) = new IncludedFields(includeId, included.toSet)
    def apply(included : TraversableOnce[String]) = new IncludedFields(WithId, included.toSet)
    def apply(includeId : IncludeIdFlag, included : TraversableOnce[String]) = new IncludedFields(includeId, included.toSet)
}

class ExcludedFields(override val excluded : Set[String]) extends Fields {
    override val included : Set[String] = Set.empty
}

object ExcludedFields {
    def apply(excluded : String*) = new ExcludedFields(excluded.toSet)
    def apply(excluded : TraversableOnce[String]) = new ExcludedFields(excluded.toSet)
}

sealed trait QueryFlag
case object QueryTailable extends QueryFlag
case object QuerySlaveOk extends QueryFlag
case object QueryOpLogReplay extends QueryFlag
case object QueryNoTimeout extends QueryFlag
case object QueryAwaitData extends QueryFlag
case object QueryExhaust extends QueryFlag

case class CountOptions(fields : Option[Fields], skip : Option[Long], limit : Option[Long], overrideQueryFlags : Option[Set[QueryFlag]])
private object CountOptions {
    final val empty = CountOptions(None, None, None, None)
}

case class DistinctOptions[+QueryType](query : Option[QueryType], overrideQueryFlags : Option[Set[QueryFlag]]) {
    def convert[AnotherQueryType](converter : QueryType => AnotherQueryType) =
        DistinctOptions[AnotherQueryType](query map { converter(_) }, overrideQueryFlags)
}

private object DistinctOptions {
    private final val _empty = DistinctOptions[Nothing](None, None)
    def empty[QueryType] : DistinctOptions[QueryType] = _empty
}

// for find(), fields on the wire level is a separate object; while for findAndModify for example
// it goes in the query/command object sort of like orderby in find().
// for now, FindOptions doesn't include the stuff that you can put in the query object
// by putting the query under a "query : {}" key, but FindAndModifyOptions is
// assumes that the query object passed in is only the query
// not sure how to sort this out yet.
case class FindOptions(fields : Option[Fields], skip : Option[Long], limit : Option[Long], batchSize : Option[Int], overrideQueryFlags : Option[Set[QueryFlag]])
private object FindOptions {
    final val empty = FindOptions(None, None, None, None, None)
}

case class FindOneOptions(fields : Option[Fields], overrideQueryFlags : Option[Set[QueryFlag]])
private object FindOneOptions {
    final val empty = FindOneOptions(None, None)
}

case class FindOneByIdOptions(fields : Option[Fields], overrideQueryFlags : Option[Set[QueryFlag]])
private object FindOneByIdOptions {
    final val empty = FindOneByIdOptions(None, None)
}

sealed trait FindAndModifyFlag
case object FindAndModifyRemove extends FindAndModifyFlag
case object FindAndModifyNew extends FindAndModifyFlag
case object FindAndModifyUpsert extends FindAndModifyFlag

case class FindAndModifyOptions[+QueryType](sort : Option[QueryType], fields : Option[Fields],
    flags : Set[FindAndModifyFlag]) {
    def convert[AnotherQueryType](converter : QueryType => AnotherQueryType) =
        FindAndModifyOptions[AnotherQueryType](sort map { converter(_) }, fields, flags)
}

private object FindAndModifyOptions {
    private final val _empty = FindAndModifyOptions[Nothing](None, None, Set.empty)
    def empty[QueryType] : FindAndModifyOptions[QueryType] = _empty
    private final val _remove = FindAndModifyOptions[Nothing](None, None, Set(FindAndModifyRemove))
    def remove[QueryType] : FindAndModifyOptions[QueryType] = _remove
}

sealed trait UpdateFlag
case object UpdateUpsert extends UpdateFlag
case object UpdateMulti extends UpdateFlag

case class UpdateOptions(flags : Set[UpdateFlag])

private object UpdateOptions {
    final val empty = UpdateOptions(Set.empty)
    final val upsert = UpdateOptions(Set(UpdateUpsert))
    final val multi = UpdateOptions(Set(UpdateMulti))
}

/**
 * Trait expressing all data-access operations on a collection in synchronous form.
 * This trait does not include setup and teardown operations such as creating or
 * removing indexes; you would use the underlying API such as Casbah or Hammersmith
 * for that, for now.
 *
 * The recommended way to obtain an instance of [[org.beaucatcher.mongo.SyncDAO]]
 * is from the `syncDAO` property on [[org.beaucatcher.mongo.CollectionOperations]],
 * which would in turn be implemented by the companion object of a case class
 * representing an object in a collection. For example you might have `case class Foo`
 * representing objects in the `foo` collection, with a companion object `object Foo`
 * which implements [[org.beaucatcher.mongo.CollectionOperations]]. You would then
 * write code such as:
 * {{{
 *    Foo.syncDAO[BObject].find() // obtain results as a BObject
 *    Foo.syncDAO[Foo].find()     // obtain results as a case class instance
 *    Foo.syncDAO.count()         // entity type not relevant
 * }}}
 * This is only a convention though, of course there's more than one way to do it.
 *
 * @define findAndModifyVsUpdate
 *
 *   findAndModify(), findAndReplace() will affect only one object and will return
 *   either the old or the new object depending on whether you specify the
 *   [[org.beaucatcher.mongo.FindAndModifyNew]] flag. update() and its variants
 *   do not return an object, and with updateMulti() you can modify multiple
 *   objects at once.
 *
 * @define findAndReplaceDocs
 *
 *   Finds the first object matching the query and updates it with the
 *   values from the provided entity. However, the ID is not updated.
 *   Returns the old object, or the new one if the
 *   [[org.beaucatcher.mongo.FindAndModifyNew]] flag was provided.
 *   If multiple objects match the query, then the `sort` parameter
 *   determines which will be replaced; only one object is ever replaced.
 *   A sort object is a map from fields to the integer "1" for ascending,
 *   "-1" for descending.
 *   If specified, the `fields` parameter determines which fields are
 *   returned in the old (or new) object returned from the method.
 */
abstract trait SyncDAO[QueryType, EntityType, IdType, ValueType] {
    def emptyQuery : QueryType

    // this causes the backend to see None instead of generating an
    // empty Fields BSON object (even though the empty object would
    // work fine)
    private def fieldsToOption(fields : Fields) : Option[Fields] = {
        fields match {
            case AllFields => None
            case _ => Some(fields)
        }
    }

    final def count() : Long =
        count(emptyQuery)
    final def count[A <% QueryType](query : A) : Long =
        count(query : QueryType, CountOptions.empty)
    final def count[A <% QueryType](query : A, fields : Fields) : Long =
        count(query : QueryType, CountOptions(fieldsToOption(fields), None, None, None))

    def count(query : QueryType, options : CountOptions) : Long

    // FIXME shouldn't distinct return an iterator not a seq for scalability (so it can be lazy?)
    final def distinct(key : String) : Seq[ValueType] =
        distinct(key, DistinctOptions.empty)
    final def distinct[A <% QueryType](key : String, query : A) : Seq[ValueType] =
        distinct(key, DistinctOptions[QueryType](Some(query), None))

    def distinct(key : String, options : DistinctOptions[QueryType]) : Seq[ValueType]

    final def find() : Iterator[EntityType] =
        find(emptyQuery, FindOptions.empty)
    final def find[A <% QueryType](query : A) : Iterator[EntityType] =
        find(query : QueryType, FindOptions.empty)
    final def find[A <% QueryType](query : A, fields : Fields) : Iterator[EntityType] =
        find(query : QueryType, FindOptions(fieldsToOption(fields), None, None, None, None))
    final def find[A <% QueryType](query : A, fields : Fields, skip : Long, limit : Long, batchSize : Int) : Iterator[EntityType] =
        find(query : QueryType, FindOptions(fieldsToOption(fields), Some(skip), Some(limit), Some(batchSize), None))

    def find(query : QueryType, options : FindOptions) : Iterator[EntityType]

    final def findOne() : Option[EntityType] =
        findOne(emptyQuery, FindOneOptions.empty)
    final def findOne[A <% QueryType](query : A) : Option[EntityType] =
        findOne(query : QueryType, FindOneOptions.empty)
    final def findOne[A <% QueryType](query : A, fields : Fields) : Option[EntityType] =
        findOne(query : QueryType, FindOneOptions(fieldsToOption(fields), None))

    def findOne(query : QueryType, options : FindOneOptions) : Option[EntityType]

    final def findOneById(id : IdType) : Option[EntityType] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById(id : IdType, fields : Fields) : Option[EntityType] =
        findOneById(id, FindOneByIdOptions(fieldsToOption(fields), None))

    def findOneById(id : IdType, options : FindOneByIdOptions) : Option[EntityType]

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     *
     * @param query query to find the object
     * @param o object with new values
     * @return old object
     */
    final def findAndReplace[A <% QueryType](query : A, o : EntityType) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions.empty)

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[A <% QueryType](query : A, o : EntityType, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](None, None, flags))

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](Some(sort), None, Set.empty))

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     */
    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](Some(sort), None, flags))

    /**
     * $findAndReplaceDocs
     *
     * $findAndModifyVsUpdate
     *
     * @param query query to find the object
     * @param o object with new values
     * @param sort sort object
     * @param fields fields to include in returned object
     * @param flags may include [[org.beaucatcher.mongo.FindAndModifyNew]] to return new rather than old object
     * @return old or new object according to flags
     */
    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B, fields : Fields, flags : Set[FindAndModifyFlag] = Set.empty) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](Some(sort), fieldsToOption(fields), flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType](query : A, modifier : B) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions.empty)

    /**
     * $findAndModifyVsUpdate
     */

    final def findAndModify[A <% QueryType, B <% QueryType](query : A, modifier : B, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](None, None, flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](Some(sort), None, Set.empty))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](Some(sort), None, flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C, fields : Fields, flags : Set[FindAndModifyFlag] = Set.empty) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](Some(sort), fieldsToOption(fields), flags))

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[A <% QueryType](query : QueryType) : Option[EntityType] =
        findAndModify(query : QueryType, None, FindAndModifyOptions.remove)

    /**
     * $findAndModifyVsUpdate
     */
    final def findAndRemove[A <% QueryType, B <% QueryType](query : A, sort : B) : Option[EntityType] =
        findAndModify(query : QueryType, None, FindAndModifyOptions[QueryType](Some(sort), None, Set(FindAndModifyRemove)))

    /** An upsertable object generally must have all fields and must have an ID. */
    def entityToUpsertableObject(entity : EntityType) : QueryType
    /**
     * A modifier object generally must not have an ID, and may be missing fields that
     * won't be modified.
     */
    def entityToModifierObject(entity : EntityType) : QueryType
    /**
     * Returns a query that matches only the given entity,
     * such as a query for the entity's ID.
     */
    def entityToUpdateQuery(entity : EntityType) : QueryType

    /**
     * $findAndModifyVsUpdate
     */
    def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Option[EntityType]

    /**
     * Adds a new object to the collection. It's an error if an object with the same ID already exists.
     */
    def insert(o : EntityType) : WriteResult

    /**
     * Does an updateUpsert() on the object.
     *
     * Unlike save() in Casbah, does not look at mutable isNew() flag
     * in the ObjectId if there's an ObjectId so it never does an insert().
     * I guess this may be less efficient, but ObjectId.isNew() seems like
     * a total hack to me. Would rather just require people to use insert()
     * if they just finished creating the object.
     */
    final def save(o : EntityType) : WriteResult =
        updateUpsert(entityToUpdateQuery(o), entityToUpsertableObject(o))
    /**
     * $findAndModifyVsUpdate
     */
    final def update[A <% QueryType](query : A, o : EntityType) : WriteResult =
        update(query : QueryType, entityToModifierObject(o), UpdateOptions.empty)
    /**
     * $findAndModifyVsUpdate
     */
    final def updateUpsert[A <% QueryType](query : A, o : EntityType) : WriteResult =
        update(query : QueryType, entityToUpsertableObject(o), UpdateOptions.upsert)

    /* Note: multi updates are not allowed with a replacement object, only with
     * "dollar sign operator" modifier objects. So there is no updateMulti overload
     * taking an entity object.
     */

    /**
     * $findAndModifyVsUpdate
     */
    final def update[A <% QueryType, B <% QueryType](query : A, modifier : B) : WriteResult =
        update(query : QueryType, modifier, UpdateOptions.empty)
    /**
     * $findAndModifyVsUpdate
     */
    final def updateUpsert[A <% QueryType, B <% QueryType](query : A, modifier : B) : WriteResult =
        update(query : QueryType, modifier, UpdateOptions.upsert)
    /**
     * Note that updating with the [[org.beaucatcher.mongo.UpdateMulti]] flag only works if you
     * do a "dollar sign operator," the modifier object can't just specify new field values. This is a
     * MongoDB thing, not a library/driver thing.
     *
     * $findAndModifyVsUpdate
     */
    final def updateMulti[A <% QueryType, B <% QueryType](query : A, modifier : B) : WriteResult =
        update(query : QueryType, modifier, UpdateOptions.multi)
    /**
     * $findAndModifyVsUpdate
     */
    def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : WriteResult

    /**
     * Deletes all objects matching the query.
     */
    def remove(query : QueryType) : WriteResult

    /**
     * Deletes the object with the given ID, if any.
     */
    def removeById(id : IdType) : WriteResult
}
