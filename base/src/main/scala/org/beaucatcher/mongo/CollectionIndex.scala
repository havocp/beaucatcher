package org.beaucatcher.mongo

/**
 * Entity representing an object in the system.indexes collection.
 */
case class CollectionIndex(name : String, ns : String, key : Map[String, Any], v : Option[Int],
    unique : Option[Boolean], background : Option[Boolean], dropDups : Option[Boolean],
    sparse : Option[Boolean]) {

}
