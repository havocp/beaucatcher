package org.beaucatcher.bson

import scala.collection.mutable

class BadSelectorException(message : String, cause : Throwable = null) extends Exception(message, cause)

private[bson] object Selector {
    private[bson] def headAndTail(selector : String) : (String, String) = {
        if (selector.length == 0) {
            ("", "")
        } else if (selector.charAt(0) == '/') {
            val end = if (selector.length > 1 && selector.charAt(1) == '/') {
                2
            } else {
                1
            }
            (selector.substring(0, end), selector.substring(end))
        } else {
            val end = selector.indexOf('/')
            if (end < 0) {
                (selector, "")
            } else {
                (selector.substring(0, end), selector.substring(end))
            }
        }
    }

    private[bson] def splitSelector(selector : String) : List[String] = {
        headAndTail(selector) match {
            case ("", _) =>
                Nil
            case (head, tail) =>
                head :: splitSelector(tail)
        }
    }

    private[bson] def checkName(name : String) : Unit = {
        if (name.startsWith("/"))
            throw new BadSelectorException("Invalid selector has more than two '/' in a row")
    }

    // arrays are treated like objects indexed with numbers, so
    // "/foo/10" means "10th element of array foo"
    private[bson] def arrayIndex(a : ArrayBase[BValue], s : String) : Option[BValue] = {
        try {
            val i = Integer.parseInt(s)
            Some(a(i))
        } catch {
            case e : NumberFormatException =>
                None
            case e : IndexOutOfBoundsException =>
                None
        }
    }

    private[bson] def matchingChildren(context : BValue, name : String) : TraversableOnce[BValue] = {
        checkName(name)
        if (name == ".") {
            Iterator(context)
        } else {
            context match {
                case o : ObjectBase[_, _] =>
                    if (name == "*")
                        o.values
                    else
                        o.get(name)
                case a : ArrayBase[_] =>
                    if (name == "*")
                        a.iterator
                    else
                        arrayIndex(a, name)
                case _ =>
                    Nil
            }
        }
    }

    private[bson] def findDescendants(context : BValue, name : String) : List[BValue] = {
        checkName(name)
        val builder = List.newBuilder[BValue]
        def findDescendantsHelper(context : BValue, name : String, builder : mutable.Builder[BValue, List[BValue]]) : Unit = {
            context match {
                case o : ObjectBase[_, _] =>
                    matchingChildren(o, name) foreach { c =>
                        builder += c
                    }

                    o.foreach({ (kv) =>
                        findDescendantsHelper(kv._2, name, builder)
                    })
                case a : ArrayBase[_] =>
                    matchingChildren(a, name) foreach { c =>
                        builder += c
                    }

                    a.foreach({ (elem) =>
                        findDescendantsHelper(elem, name, builder)
                    })
                case v : BValue =>
                    if (name == ".")
                        builder += v
            }
        }
        findDescendantsHelper(context, name, builder)
        builder.result
    }

    private[bson] def findChildren(context : BValue, name : String) : List[BValue] = {
        matchingChildren(context, name).toList
    }

    private[bson] def select(contexts : List[BValue], split : List[String]) : List[BValue] = {
        contexts flatMap { context =>
            context match {
                case o : ObjectBase[_, _] =>
                    select(o, split)
                case a : ArrayBase[_] =>
                    select(a, split)
                case _ =>
                    Nil
            }
        }
    }

    private[bson] def select(context : BValue, split : List[String]) : List[BValue] = {
        split match {
            // Check valid ends of the selector, returning the results
            // and ending recursion. Only the last step in the selector
            // actually matches results we want to return.
            case "/" :: childName :: Nil =>
                findChildren(context, childName)
            case "//" :: childName :: Nil =>
                findDescendants(context, childName)
            case childName :: Nil =>
                findChildren(context, childName)

            // recurse deeper because we aren't at the end.
            case "/" :: childName :: tail =>
                select(findChildren(context, childName), tail)
            case "//" :: childName :: tail =>
                select(findDescendants(context, childName), tail)
            case childName :: tail =>
                select(findChildren(context, childName), tail)

            // should not happen
            case Nil =>
                throw new IllegalStateException("Invalid selector was not detected in validateSplit()")
        }
    }

    private[bson] def validateSplit(split : List[String]) : Unit = {
        split match {
            case childName :: "/" :: Nil =>
                throw new BadSelectorException("Invalid selector ends with '/'")
            case childName :: "//" :: Nil =>
                throw new BadSelectorException("Invalid selector ends with '//'")
            case Nil =>
                throw new BadSelectorException("Invalid empty selector")
            case "/" :: tail =>
                throw new BadSelectorException("Invalid selector starts with '/' (can't start with parent since BValue has no parent reference)")
            case "//" :: tail =>
                throw new BadSelectorException("Invalid selector starts with '//' (can't start with parent since BValue has no parent reference)")
            case childName :: Nil => // done and OK
            case childName :: "/" :: tail =>
                validateSplit(tail)
            case childName :: "//" :: tail =>
                validateSplit(tail)
            case _ =>
                throw new IllegalStateException("selector validation case should not be reached")
        }
    }

    def select(context : BValue, selector : String) : List[BValue] = {
        try {
            val split = splitSelector(selector)
            validateSplit(split)
            select(context :: Nil, split)
        } catch {
            case e : BadSelectorException =>
                // add the problematic selector to the error. This is done here on the outermost
                // part of the stack so we show the entire selector.
                throw new BadSelectorException(e.getMessage + " '" + selector + "'", e)
        }
    }

    /**
     * Selects values of type `A` matching the given selector.
     * If nodes matching the selector have the wrong type, they are
     * simply filtered out (no exception is thrown).
     */
    def selectAs[A : Manifest](context : BValue, selector : String) : List[A] = {
        select(context, selector) flatMap { v =>
            try {
                v.unwrappedAs[A] :: Nil
            } catch {
                case e : ClassCastException =>
                    Nil
            }
        }
    }
}
