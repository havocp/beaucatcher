package org.beaucatcher.mongo

import com.mongodb.WriteResult
import org.beaucatcher.bson._

/* This is really oversimplified for now; there are a ton of options
 * for each request and of course we want async sometime. Keeping
 * it simple just to figure things out before adding a lot of noise.
 * 
 * The type parameters are invariant because they
 * occur in both covariant and contravariant positions. I think 
 * it could be split up so there were 2x the number of type 
 * parameters, for example QueryType becomes CoQueryType and ContraQueryType,
 * but it gets really confusing and I don't know if there's 
 * really any utility to it.
 */
abstract trait SyncDAO[QueryType, EntityType, IdType] {
    def find[A <% QueryType](ref : A) : Iterator[EntityType]

    def findOne[A <% QueryType](t : A) : Option[EntityType]
    def findOneByID(id : IdType) : Option[EntityType]

    def findAndModify[A <% QueryType](q : A, t : EntityType) : Option[EntityType]

    def save(t : EntityType) : WriteResult
    def insert(t : EntityType) : WriteResult

    def update[A <% QueryType](q : A, o : EntityType) : WriteResult

    def remove(t : EntityType) : WriteResult
}

