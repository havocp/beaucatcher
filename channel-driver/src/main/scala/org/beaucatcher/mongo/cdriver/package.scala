package org.beaucatcher.mongo

import org.beaucatcher.channel._
import org.beaucatcher.bson._
import org.beaucatcher.mongo._
import org.beaucatcher.driver._

package object cdriver {

    private[cdriver] class EnrichedContext(context: DriverContext) {
        def asChannelContext() = {
            context match {
                case null =>
                    throw new BugInSomethingMongoException("null mongo.Context")
                case j: ChannelDriverContext =>
                    j
                case wrong =>
                    throw new BugInSomethingMongoException("mongo.Context passed to cdriver is not from cdriver; context type is " + wrong.getClass.getSimpleName)
            }
        }
    }

    private[cdriver] implicit def context2enriched(context: DriverContext) = new EnrichedContext(context)

    private[cdriver] def queryFlags(flags: Option[Set[QueryFlag]]): Int = {
        queryFlagsAsInt(flags.getOrElse(Set.empty[QueryFlag]))
    }

    private def asBoolean(a: Option[Any]): Option[Boolean] = {
        a.flatMap({
            case null =>
                None
            case x: Boolean =>
                Some(x)
            case x: Number =>
                Some(x.intValue() == 1)
            case wtf =>
                None
        })
    }

    private def deNull[T](a: Option[T]): Option[T] = {
        if (a == Some(null))
            None
        else
            a
    }

    private def newRawQueryResultDecoder[E](fields: RawField*)(implicit entitySupport: QueryResultDecoder[E]): QueryResultDecoder[RawDecoded] = {
        RawDecoded.rawQueryResultDecoderFields[E](Seq("ok", "errmsg", "err", "code",
            "n", "upserted", "updatedExisting").map(RawField(_, None)) ++ fields)
    }

    private[cdriver] def decodeCommandResultFields[E](reply: QueryReply, fields: RawField*)(implicit entitySupport: QueryResultDecoder[E]): DecodedResult = {
        import CodecUtils._

        // BObject here isn't expected to be actually used because
        // none of the fields we are fetching should have type object
        // TODO we could add a type which throws an exception if it's
        // ever decoded.
        implicit val decoder = newRawQueryResultDecoder[E](fields: _*)
        val doc = reply.iterator[RawDecoded]().next()
        val errOption = deNull(doc.fields.get("err")).map(_.asInstanceOf[String])
        // getLastError returns ok=true and err!=null if there was an error,
        // here we convert that into ok=false so ok reliably tells us if we
        // have an error.
        val ok = errOption.isEmpty && asBoolean(doc.fields.get("ok")).getOrElse(throw new MongoException("no 'ok' field in command result: " + doc.fields))
        val errmsgOption = deNull(doc.fields.get("errmsg")).map(_.asInstanceOf[String])

        val codeOption = deNull(doc.fields.get("code")).map(_.asInstanceOf[Number].intValue)

        DecodedResult(CommandResult(ok = ok, errmsg = errmsgOption, err = errOption, code = codeOption), doc.fields)
    }

    private[cdriver] def decodeCommandResult[E](reply: QueryReply, fields: String*)(implicit entitySupport: QueryResultDecoder[E]): DecodedResult = {
        decodeCommandResultFields(reply, fields.map(RawField(_, None)): _*)
    }

    private[cdriver] def decodeWriteResult[E](reply: QueryReply, fields: String*)(implicit entitySupport: QueryResultDecoder[E]): DecodedWriteResult = {
        val command = decodeCommandResult(reply, fields: _*)

        val n = deNull(command.fields.get("n")).map(_.asInstanceOf[Number].intValue).getOrElse(0)
        val upsertedOption = deNull(command.fields.get("upserted")).map(_.asInstanceOf[AnyRef])
        val updatedExistingOption = asBoolean(command.fields.get("updatedExisting"))

        DecodedWriteResult(WriteResult(command.result, n = n, upserted = upsertedOption, updatedExisting = updatedExistingOption),
            command.fields)
    }
}
