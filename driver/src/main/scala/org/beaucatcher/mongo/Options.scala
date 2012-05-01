package org.beaucatcher.mongo

sealed trait Fields {
    val included: Set[String]
    val excluded: Set[String]

    override def toString = "Fields(included=%s,excluded=%s)".format(included, excluded)

    // this causes the backend to see None instead of generating an
    // empty Fields BSON object (even though the empty object would
    // work fine)
    private[beaucatcher] def toOption: Option[Fields] = {
        this match {
            case AllFields => None
            case _ => Some(this)
        }
    }
}

sealed trait IncludedFieldsIdFlag
case object FieldsWithId extends IncludedFieldsIdFlag
case object FieldsWithoutId extends IncludedFieldsIdFlag

object AllFields extends Fields {
    override val included: Set[String] = Set.empty
    override val excluded: Set[String] = Set.empty

    override def toString = "AllFields"
}

class IncludedFields(includeId: IncludedFieldsIdFlag, override val included: Set[String]) extends Fields {
    override val excluded: Set[String] = if (includeId == FieldsWithId) Set.empty else Set("_id")
}

object IncludedFields {
    lazy val idOnly = new IncludedFields(FieldsWithId, Set.empty)
    def apply(included: String*) = new IncludedFields(FieldsWithId, included.toSet)
    def apply(includeId: IncludedFieldsIdFlag, included: String*) = new IncludedFields(includeId, included.toSet)
    def apply(included: TraversableOnce[String]) = new IncludedFields(FieldsWithId, included.toSet)
    def apply(includeId: IncludedFieldsIdFlag, included: TraversableOnce[String]) = new IncludedFields(includeId, included.toSet)
}

class ExcludedFields(override val excluded: Set[String]) extends Fields {
    override val included: Set[String] = Set.empty
}

object ExcludedFields {
    def apply(excluded: String*) = new ExcludedFields(excluded.toSet)
    def apply(excluded: TraversableOnce[String]) = new ExcludedFields(excluded.toSet)
}

sealed trait QueryFlag
case object QueryTailable extends QueryFlag
case object QuerySlaveOk extends QueryFlag
case object QueryOpLogReplay extends QueryFlag
case object QueryNoTimeout extends QueryFlag
case object QueryAwaitData extends QueryFlag
case object QueryExhaust extends QueryFlag

case class CountOptions(fields: Option[Fields] = None, skip: Option[Long] = None, limit: Option[Long] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None)
object CountOptions {
    final val empty = CountOptions()
}

case class DistinctOptions[+Q](query: Option[Q] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None) {

}

object DistinctOptions {
    private final val _empty = DistinctOptions[Nothing]()
    def empty[Q]: DistinctOptions[Q] = _empty
}

// for find(), fields on the wire level is a separate object; while for findAndModify for example
// it goes in the query/command object sort of like orderby in find().
// for now, FindOptions doesn't include the stuff that you can put in the query object
// by putting the query under a "query : {}" key, but FindAndModifyOptions is
// assumes that the query object passed in is only the query
// not sure how to sort this out yet.
case class FindOptions(fields: Option[Fields] = None, skip: Option[Long] = None, limit: Option[Long] = None, batchSize: Option[Int] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None)
object FindOptions {
    final val empty = FindOptions()
}

case class FindOneOptions(fields: Option[Fields] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None)
object FindOneOptions {
    final val empty = FindOneOptions()
}

case class FindOneByIdOptions(fields: Option[Fields] = None, overrideQueryFlags: Option[Set[QueryFlag]] = None)
object FindOneByIdOptions {
    final val empty = FindOneByIdOptions()
}

sealed trait FindAndModifyFlag
case object FindAndModifyRemove extends FindAndModifyFlag
case object FindAndModifyNew extends FindAndModifyFlag
case object FindAndModifyUpsert extends FindAndModifyFlag

case class FindAndModifyOptions[+S](sort: Option[S] = None, fields: Option[Fields] = None,
    flags: Set[FindAndModifyFlag] = Set.empty) {
}

object FindAndModifyOptions {
    private final val _empty = FindAndModifyOptions[Nothing]()
    def empty[S]: FindAndModifyOptions[S] = _empty
    private final val _remove = FindAndModifyOptions[Nothing](flags = Set(FindAndModifyRemove))
    def remove[S]: FindAndModifyOptions[S] = _remove
}

sealed trait UpdateFlag
case object UpdateUpsert extends UpdateFlag
case object UpdateMulti extends UpdateFlag

case class UpdateOptions(flags: Set[UpdateFlag] = Set.empty)

object UpdateOptions {
    final val empty = UpdateOptions()
    final val upsert = UpdateOptions(flags = Set(UpdateUpsert))
    final val multi = UpdateOptions(flags = Set(UpdateMulti))
}

sealed trait IndexFlag
case object IndexUnique extends IndexFlag
case object IndexBackground extends IndexFlag
case object IndexDropDups extends IndexFlag
case object IndexSparse extends IndexFlag

case class IndexOptions(name: Option[String] = None, flags: Set[IndexFlag] = Set.empty, v: Option[Int] = None)

object IndexOptions {
    val empty = IndexOptions()
}

case class CommandOptions(overrideQueryFlags: Option[Set[QueryFlag]] = None)

private[beaucatcher] object CommandOptions {
    final val empty = CommandOptions(None)
}
