package org.beaucatcher.mongo

import org.beaucatcher.bson._

/**
 * Entity representing an object in the system.indexes collection.
 */
case class CollectionIndex(name : String, ns : String, key : BObject, v : Option[Int],
    unique : Option[Boolean], background : Option[Boolean], dropDups : Option[Boolean],
    sparse : Option[Boolean]) {

}
