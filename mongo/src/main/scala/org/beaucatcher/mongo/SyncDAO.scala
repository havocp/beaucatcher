package org.beaucatcher.mongo

import com.mongodb.WriteResult
import org.beaucatcher.bson._

sealed trait Fields {
    val included : Set[String]
    val excluded : Set[String]
}

sealed trait IncludeIdFlag
case object WithId extends IncludeIdFlag
case object WithoutId extends IncludeIdFlag

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

case class CountOptions(fields : Option[Fields])
private object CountOptions {
    final val empty = CountOptions(None)
}

case class DistinctOptions[+QueryType](query : Option[QueryType]) {
    def convert[AnotherQueryType](converter : QueryType => AnotherQueryType) =
        DistinctOptions[AnotherQueryType](query map { converter(_) })
}

private object DistinctOptions {
    final val _empty = DistinctOptions[Nothing](None)
    def empty[QueryType] : DistinctOptions[QueryType] = _empty
}

// FIXME I think we need limit here since it isn't on cursor
case class FindOptions(fields : Option[Fields], numToSkip : Option[Int], batchSize : Option[Int])
private object FindOptions {
    final val empty = FindOptions(None, None, None)
}

case class FindOneOptions(fields : Option[Fields])
private object FindOneOptions {
    final val empty = FindOneOptions(None)
}

case class FindOneByIdOptions(fields : Option[Fields])
private object FindOneByIdOptions {
    final val empty = FindOneByIdOptions(None)
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
    final val _empty = FindAndModifyOptions[Nothing](None, None, Set.empty)
    def empty[QueryType] : FindAndModifyOptions[QueryType] = _empty
    final val _remove = FindAndModifyOptions[Nothing](None, None, Set(FindAndModifyRemove))
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
 * The type parameters are invariant because they
 * occur in both covariant and contravariant positions. I think
 * it could be split up so there were 2x the number of type
 * parameters, for example QueryType becomes CoQueryType and ContraQueryType,
 * but it gets really confusing and I don't know if there's
 * really any utility to it.
 */
abstract trait SyncDAO[QueryType, EntityType, IdType, ValueType] {
    def emptyQuery : QueryType

    final def count() : Long =
        count(emptyQuery)
    final def count[A <% QueryType](query : A) : Long =
        count(query : QueryType, CountOptions.empty)
    final def count[A <% QueryType](query : A, fields : Fields) : Long =
        count(query : QueryType, CountOptions(Some(fields)))

    def count(query : QueryType, options : CountOptions) : Long

    final def distinct(key : String) : Seq[ValueType] =
        distinct(key, DistinctOptions.empty)
    final def distinct[A <% QueryType](key : String, query : A) : Seq[ValueType] =
        distinct(key, DistinctOptions[QueryType](Some(query)))

    def distinct(key : String, options : DistinctOptions[QueryType]) : Seq[ValueType]

    final def find() : Iterator[EntityType] =
        find(emptyQuery, FindOptions.empty)
    final def find[A <% QueryType](query : A) : Iterator[EntityType] =
        find(query : QueryType, FindOptions.empty)
    final def find[A <% QueryType](query : A, fields : Fields) : Iterator[EntityType] =
        find(query : QueryType, FindOptions(Some(fields), None, None))
    final def find[A <% QueryType](query : A, fields : Fields, numToSkip : Int, batchSize : Int) : Iterator[EntityType] =
        find(query : QueryType, FindOptions(Some(fields), Some(numToSkip), Some(batchSize)))

    def find(query : QueryType, options : FindOptions) : Iterator[EntityType]

    final def findOne() : Option[EntityType] =
        findOne(emptyQuery, FindOneOptions.empty)
    final def findOne[A <% QueryType](query : A) : Option[EntityType] =
        findOne(query : QueryType, FindOneOptions.empty)
    final def findOne[A <% QueryType](query : A, fields : Fields) : Option[EntityType] =
        findOne(query : QueryType, FindOneOptions(Some(fields)))

    def findOne(query : QueryType, options : FindOneOptions) : Option[EntityType]

    final def findOneById(id : IdType) : Option[EntityType] =
        findOneById(id, FindOneByIdOptions.empty)
    final def findOneById(id : IdType, fields : Fields) : Option[EntityType] =
        findOneById(id, FindOneByIdOptions(Some(fields)))

    def findOneById(id : IdType, options : FindOneByIdOptions) : Option[EntityType]

    final def findAndReplace[A <% QueryType](query : A, o : EntityType) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions.empty)

    final def findAndReplace[A <% QueryType](query : A, o : EntityType, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](None, None, flags))

    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](Some(sort), None, Set.empty))

    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](Some(sort), None, flags))

    final def findAndReplace[A <% QueryType, B <% QueryType](query : A, o : EntityType, sort : B, fields : Fields, flags : Set[FindAndModifyFlag] = Set.empty) : Option[EntityType] =
        findAndModify(query : QueryType, Some(entityToModifierObject(o)),
            FindAndModifyOptions[QueryType](Some(sort), Some(fields), flags))

    final def findAndModify[A <% QueryType, B <% QueryType](query : A, modifier : B) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions.empty)

    final def findAndModify[A <% QueryType, B <% QueryType](query : A, modifier : B, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](None, None, flags))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](Some(sort), None, Set.empty))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C, flags : Set[FindAndModifyFlag]) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](Some(sort), None, flags))

    final def findAndModify[A <% QueryType, B <% QueryType, C <% QueryType](query : A, modifier : B, sort : C, fields : Fields, flags : Set[FindAndModifyFlag] = Set.empty) : Option[EntityType] =
        findAndModify(query : QueryType, Some(modifier : QueryType),
            FindAndModifyOptions[QueryType](Some(sort), Some(fields), flags))

    final def findAndRemove[A <% QueryType](query : QueryType) : Option[EntityType] =
        findAndModify(query : QueryType, None, FindAndModifyOptions.remove)

    final def findAndRemove[A <% QueryType, B <% QueryType](query : A, sort : B) : Option[EntityType] =
        findAndModify(query : QueryType, None, FindAndModifyOptions[QueryType](Some(sort), None, Set(FindAndModifyRemove)))

    def entityToModifierObject(entity : EntityType) : QueryType
    def findAndModify(query : QueryType, update : Option[QueryType], options : FindAndModifyOptions[QueryType]) : Option[EntityType]

    def save(o : EntityType) : WriteResult
    def insert(o : EntityType) : WriteResult

    final def update[A <% QueryType](query : A, o : EntityType) : WriteResult =
        update(query : QueryType, entityToModifierObject(o), UpdateOptions.empty)
    final def updateUpsert[A <% QueryType](query : A, o : EntityType) : WriteResult =
        update(query : QueryType, entityToModifierObject(o), UpdateOptions.upsert)
    final def updateMulti[A <% QueryType](query : A, o : EntityType) : WriteResult =
        update(query : QueryType, entityToModifierObject(o), UpdateOptions.multi)

    final def update[A <% QueryType, B <% QueryType](query : A, modifier : B) : WriteResult =
        update(query : QueryType, modifier, UpdateOptions.empty)
    final def updateUpsert[A <% QueryType, B <% QueryType](query : A, modifier : B) : WriteResult =
        update(query : QueryType, modifier, UpdateOptions.upsert)
    final def updateMulti[A <% QueryType, B <% QueryType](query : A, modifier : B) : WriteResult =
        update(query : QueryType, modifier, UpdateOptions.multi)

    def update(query : QueryType, modifier : QueryType, options : UpdateOptions) : WriteResult

    def remove(query : QueryType) : WriteResult
    def removeById(id : IdType) : WriteResult
}
