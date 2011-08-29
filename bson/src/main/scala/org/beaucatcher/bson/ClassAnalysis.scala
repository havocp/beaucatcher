/**
 * Copyright 2011 Havoc Pennington
 * derived from Salat,
 * Copyright 2010-2011 Novus Partners, Inc. <http://novus.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.beaucatcher.bson

import java.lang.annotation.Annotation
import java.lang.reflect.{InvocationTargetException, Constructor, Method, AnnotatedElement}
import scala.annotation.target.getter
import scala.reflect.generic.ByteCodecs
import scala.reflect.ScalaSignature
import scala.tools.scalap.scalax.rules.scalasig._

/**
 * An analysis (using reflection) of the fields in a case class.
 *
 * You can then iterate over field names and values, and convert the
 * case class to and from a [[scala.collection.Map]].
 */
class ClassAnalysis[X <: ClassAnalysis.CaseClass](private val clazz : Class[X]) {
    import ClassAnalysis._

    /**
     * Get field names in the object.
     */
    def fieldNamesIterator : Iterator[String] = {
        indexedFields.iterator.map(_.name)
    }

    /**
     * Iterate over the types of fields.
     */
    def fieldTypesIterator : Iterator[Type] = {
        indexedFields.iterator.map(_.typeRefType)
    }

    /**
     * Iterate over whether the fields are optional
     */
    def fieldOptionalityIterator : Iterator[Boolean] = {
        indexedFields.iterator.map(_.optional)
    }

    /**
     * Iterate over field names and current values for an instance.
     */
    def fieldIterator(o : X) : Iterator[(String, Any)] = {
        val valuesAndFields = o.productIterator.zip(indexedFields.iterator)
        valuesAndFields.map({ valueAndField =>
            (valueAndField._2.name -> valueAndField._1)
        })
    }

    /**
     * Pull fields from an object as map from field names to values.
     */
    def asMap(o : X) : Map[String, Any] = {
        val builder = Map.newBuilder[String, Any]

        for ((name, value) <- fieldIterator(o)) {
            value match {
                case None =>
                // Option-valued field is not present, omit
                case Some(x) =>
                    // Option-valued field is present, include
                    builder += name -> x
                case _ =>
                    // regular (non optional) field
                    builder += name -> value
            }
        }

        builder.result
    }

    /**
     * Construct an object from a map of field names and values.
     */
    def fromMap(m : Map[String, Any]) : X = {
        if (sym.isModule) {
            companionObject.asInstanceOf[X]
        } else {
            val args = indexedFields.map {
                case field => {
                    val optionalValue = m.get(field.name)
                    if (field.optional) {
                        optionalValue
                    } else {
                        optionalValue match {
                            case Some(value) =>
                                value
                            case None =>
                                throw new Exception("%s requires value for '%s' map was '%s'".format(clazz, field.name, m))
                        }
                    }
                }
            }.map(_.asInstanceOf[AnyRef])

            try {
                constructor.newInstance(args : _*)
            } catch {
                // when something bad happens feeding args into constructor, catch these exceptions and
                // wrap them in a custom exception that will provide detailed information about what's happening.
                case e : InstantiationException => throw new ToObjectGlitch(this, sym, constructor, args, e)
                case e : IllegalAccessException => throw new ToObjectGlitch(this, sym, constructor, args, e)
                case e : IllegalArgumentException => throw new ToObjectGlitch(this, sym, constructor, args, e)
                case e : InvocationTargetException => throw new ToObjectGlitch(this, sym, constructor, args, e)
                case e => throw e
            }
        }
    }

    override def toString = "ClassAnalysis(%s)".format(clazz)

    override def equals(that : Any) = that.isInstanceOf[ClassAnalysis[_]] && that.asInstanceOf[ClassAnalysis[_]].sym.path == this.sym.path

    override def hashCode = sym.path.hashCode

    private def parseScalaSig[A](clazz : Class[A]) : Option[ScalaSig] = {
        val firstPass = parseScalaSig0(clazz)
        firstPass match {
            case Some(x) => {
                Some(x)
            }
            case None if clazz.getName.endsWith("$") => {
                val clayy = Class.forName(clazz.getName.replaceFirst("\\$$", ""))
                val secondPass = parseScalaSig0(clayy)
                secondPass
            }
            case x => x
        }
    }

    // this returns something like: ClassSymbol(IntAndString, owner=com.example.somepackage, flags=40, info=9 ,None)
    private def findSym[A](clazz : Class[A]) = {
        val pss = parseScalaSig(clazz)
        pss match {
            case Some(x) => {
                val topLevelClasses = x.topLevelClasses
                topLevelClasses.headOption match {
                    case Some(tlc) => {
                        //System.out.println("tlc=" + tlc)
                        tlc
                    }
                    case None => {
                        val topLevelObjects = x.topLevelObjects
                        topLevelObjects.headOption match {
                            case Some(tlo) => {
                                tlo
                            }
                            case _ => throw new MissingExpectedType(clazz)
                        }
                    }
                }
            }
            case None => throw new MissingPickledSig(clazz)
        }
    }

    private lazy val sym = findSym(clazz)

    // annotations on a getter don't actually inherit from a trait or an abstract superclass,
    // but dragging them down manually allows for much nicer behaviour - this way you can specify @Persist or @Key
    // on a trait and have it work all the way down
    private def interestingClass(clazz : Class[_]) = clazz match {
        case clazz if clazz == null => false // inconceivably, this happens!
        case clazz if clazz.getName.startsWith("java.") => false
        case clazz if clazz.getName.startsWith("javax.") => false
        case clazz if clazz.getName.startsWith("scala.") => false
        case clazz if clazz.getEnclosingClass != null => false // filter out nested traits and superclasses
        case _ => true
    }

    private lazy val interestingInterfaces : List[(Class[_], SymbolInfoSymbol)] = {
        val interfaces = clazz.getInterfaces // this should return an empty array, but...  sometimes returns null!
        if (interfaces != null && interfaces.nonEmpty) {
            val builder = List.newBuilder[(Class[_], SymbolInfoSymbol)]
            for (interface <- interfaces) {
                if (interestingClass(interface)) {
                    builder += ((interface, findSym(interface)))
                }
            }
            builder.result()
        } else Nil
    }

    private lazy val interestingSuperclass : List[(Class[_], SymbolInfoSymbol)] = clazz.getSuperclass match {
        case superClazz if interestingClass(superClazz) => List((superClazz, findSym(superClazz)))
        case _ => Nil
    }

    // for use when you just want to find something and whether it was declared in clazz, some trait clazz extends, or clazz' own superclass
    // is not a concern
    private lazy val allTheChildren : Seq[Symbol] = sym.children ++ interestingInterfaces.map(_._2.children).flatten ++ interestingSuperclass.map(_._2.children).flatten

    // sym.children would look at objects like these two for a field "foo" 
    //   MethodSymbol(foo, owner=0, flags=29400200, info=32 ,None)
    //   MethodSymbol(foo , owner=0, flags=21080004, info=33 ,None)    
    private lazy val indexedFields = {
        // don't use allTheChildren here!  this is the indexed fields for clazz and clazz alone
        sym.children
            .filter({ c => c.isCaseAccessor && !c.isPrivate })
            .map(_.asInstanceOf[MethodSymbol])
            .zipWithIndex
            .map {
                case (ms, idx) => {
                    //printf("indexedFields: clazz=%s, ms=%s, idx=%s\n", clazz, ms, idx)
                    Field(idx, ms.name, typeRefType(ms), clazz.getMethod(ms.name))
                }
            }
    }

    private lazy val companionClass = ClassAnalysis.companionClass(clazz)
    private lazy val companionObject = ClassAnalysis.companionObject(clazz)

    private lazy val constructor : Constructor[X] = {
        val cl = clazz.getConstructors.asInstanceOf[Array[Constructor[X]]].filter(_.getParameterTypes().length > 0)
        // I'm seeing two constructors, the regular case class one and one with no arguments. Above we filter the
        // no arguments one (which I don't understand) and then we get upset if we find more.
        // Case classes can have extra constructors but overloading apply() is more the usual thing to do.
        if (cl.size > 1) {
            throw new RuntimeException("constructor: clazz=%s, expected 1 constructor but found %d\n%s".format(clazz, cl.size, cl.mkString("\n")))
        }
        val c = cl.headOption.getOrElse(throw new MissingConstructor(sym))
        //printf("constructor: clazz=%s ---> constructor=%s\n", clazz, c)
        c
    }

    private def typeRefType(ms : MethodSymbol) : TypeRefType = ms.infoType match {
        // it looks like 2.8.1 used PolyType and 2.9.0 uses NullaryMethodType
        case PolyType(tr @ TypeRefType(_, _, _), _) => tr
        //case NullaryMethodType(tr @ TypeRefType(_, _, _)) => tr
        case _ => {
            throw new UnexpectedMethodSig(ms)
        }
    }
}

object ClassAnalysis {
    class UnexpectedMethodSig(ms : MethodSymbol) extends Exception("Failed to interpret method %s infoType %s".format(ms, ms.infoType))
    class MissingPickledSig(clazz : Class[_]) extends Exception("Failed to parse pickled Scala signature from: %s".format(clazz))
    class MissingExpectedType(clazz : Class[_]) extends Exception("Parsed pickled Scala signature, but no expected type found: %s"
        .format(clazz))
    class MissingTopLevelClass(clazz : Class[_]) extends Exception("Parsed pickled scala signature but found no top level class for: %s"
        .format(clazz))
    class NestingGlitch(clazz : Class[_], owner : String, outer : String, inner : String) extends Exception("Didn't find owner=%s, outer=%s, inner=%s in pickled scala sig for %s"
        .format(owner, outer, inner, clazz))
    class MissingConstructor(sym : SymbolInfoSymbol) extends Exception("Couldn't find a constructor for %s".format(sym.path))
    class ToObjectGlitch[X <: ClassAnalysis.CaseClass](classAnalysis : ClassAnalysis[X], sym : SymbolInfoSymbol, constructor : Constructor[X], args : Seq[AnyRef], cause : Throwable) extends Exception(
        """
  %s

  %s toObject failed on:
  SYM: %s
  CONSTRUCTOR: %s
  ARGS:
  %s
  """.format(cause.getMessage, classAnalysis.toString, sym.path, constructor, args), cause)

    private type CaseClass = AnyRef with Product

    private def annotation[A <: Annotation : Manifest](x : AnnotatedElement) : Option[A] = {
        x.getAnnotation[A](manifest[A].erasure.asInstanceOf[Class[A]]) match {
            case a if a != null => Some(a)
            case _ => None
        }
    }
    private def annotated_?[A <: Annotation : Manifest](x : AnnotatedElement) : Boolean = {
        annotation[A](x)(manifest[A]).isDefined
    }

    private def extractSingleTypeArg(t : Type, typeName : String) : Option[Type] = {
        t match {
            case TypeRefType(_, symbol, Seq(typeArg)) if symbol.path == typeName =>
                Some(typeArg)
            case _ => None
        }
    }

    private def companionClass(clazz : Class[_]) : Class[_] =
        Class.forName(if (clazz.getName.endsWith("$")) clazz.getName else "%s$".format(clazz.getName))

    private def companionObject(clazz : Class[_]) = companionClass(clazz).getField("MODULE$").get(null)

    private object Field {
        def apply(idx : Int, name : String, t : TypeRefType, method : Method) : Field = {
            new Field(idx, name, t)
        }
    }

    private class Field(val idx : Int, val name : String, val typeRefType : TypeRefType) {
        override def toString = "Field[%d/%s]".format(idx, name)

        val optionalType : Option[Type] = extractSingleTypeArg(typeRefType, "scala.Option")
        val optional : Boolean = optionalType.isDefined
    }

    private def parseScalaSig0(clazz : Class[_]) : Option[ScalaSig] = {
        //println("parseScalaSig0 " + clazz)
        if (clazz != null) {
            val maybeSig = try {
                // This throws an NPE if there's no class file with the
                // bytecode in it. presumably a bug in ScalaSigParser.
                // Then we recover and try the annotation instead.
                // A class loader doesn't have to use a class file,
                // that's why this can fail.
                ScalaSigParser.parse(clazz)
            } catch {
                case npe: NullPointerException =>
                    None
            }

            maybeSig match {
                case Some(sig) if sig != null => Some(sig)
                case _ => annotation[ScalaSignature](clazz) match {
                    case Some(sig) if sig != null => {
                        val bytes = sig.bytes.getBytes("UTF-8")
                        val len = ByteCodecs.decode(bytes)
                        val parsedSig = ScalaSigAttributeParsers.parse(ByteCode(bytes.take(len)))
                        Option(parsedSig)
                    }
                    case _ => {
                        //log.error("parseScalaSig: could not parse clazz='%s' from class or scala.reflect.ScalaSignature", clazz.getName)
                        None
                    }
                }
            }
        } else {
            //log.error("parseScalaSig: clazz was null!")
            None
        }
    }
}
