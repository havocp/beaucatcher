package org.beaucatcher.akka

import com.mongodb.WriteResult
import org.beaucatcher.bson._
import akka.dispatch.Future

object AsyncDAO {
    sealed trait CursorResult[+EntityType]
    case class Entry[EntityType](entity : EntityType) extends CursorResult[EntityType]
    case object EOF extends CursorResult[Nothing]

}

abstract trait AsyncDAO[QueryType, EntityType, IdType] {

    def find[A <% QueryType](ref : A) : Future[Iterator[Future[AsyncDAO.CursorResult[EntityType]]]]

    def findOne[A <% QueryType](t : A) : Future[Option[EntityType]]
    def findOneByID(id : IdType) : Future[Option[EntityType]]

    def findAndModify[A <% QueryType](q : A, t : EntityType) : Future[Option[EntityType]]

    def save(t : EntityType) : Future[WriteResult]
    def insert(t : EntityType) : Future[WriteResult]

    def update[A <% QueryType](q : A, o : EntityType) : Future[WriteResult]

    def remove(t : EntityType) : Future[WriteResult]
}

