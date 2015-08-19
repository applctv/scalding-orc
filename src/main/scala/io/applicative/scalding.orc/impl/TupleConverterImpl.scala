package io.applicative.scalding.orc.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }
import com.twitter.bijection.macros.impl.IsCaseClassImpl
/**
 * Mostly copied from Scalding-macros, but with support for more types as allowed by ORC
 *
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */

object TupleConverterImpl {
  def caseClassTupleConverterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleConverter[T]] = {
    import c.universe._

    val allowUnknownTypes: Boolean = true

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    case class Extractor(tpe: Type, toTree: Tree)
    case class Builder(toTree: Tree = q"")

    implicit val builderLiftable = new Liftable[Builder] {
      def apply(b: Builder): Tree = b.toTree
    }

    implicit val extractorLiftable = new Liftable[Extractor] {
      def apply(b: Extractor): Tree = b.toTree
    }

    def matchField(outerTpe: Type, idx: Int, inOption: Boolean, term: TermName): (Int, Extractor, List[Builder]) = {
      def getPrimitive(primitiveGetter: Tree, boxedType: Type, box: Option[Tree]): (Int, Extractor, List[Builder]) = {
        if (inOption) {
          val cachedResult = newTermName(c.fresh(s"cacheVal"))
          val boxed = box.map{ b => q"""$b($primitiveGetter)""" }.getOrElse(primitiveGetter)

          val builder = q"""
          val $cachedResult: $boxedType = if(t.getObject($idx) == null) {
              null.asInstanceOf[$boxedType]
            } else {
              $boxed
            }
          """
          (idx + 1,
            Extractor(boxedType, q"$cachedResult"),
            List(Builder(builder)))
        } else {
          (idx + 1, Extractor(outerTpe, primitiveGetter), List[Builder]())
        }
      }

      def classParameter(coreType: Type): (Int, Extractor, List[Builder]) = {
        val typParam = coreType.asInstanceOf[TypeRefApi].args.head
        typParam match {
          case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => {
            val innerList = TermName(c.freshName("innerList"))
            val cachedName = TermName(c.freshName("innerTuple"))
            val v = expandCaseClass(tpe, 0, false, cachedName)
            val cachedResult = q"""
              val $innerList = _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.util.List[_root_.java.lang.Object]](
                $term.getObject($idx).asInstanceOf[_root_.java.util.List[_root_.java.util.List[_root_.java.lang.Object]]]
              ).asScala
              $innerList.map { i =>
                val $cachedName = new _root_.cascading.tuple.Tuple(
                  _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.lang.Object](i).asScala : _*)
                ..${v._3}
                ${v._2}
              }.to[${coreType.typeConstructor}]
            """
            getPrimitive(cachedResult, coreType, None)
          }
          case _ => getPrimitive(
            q"""
              _root_.scala.collection.JavaConverters.asScalaBufferConverter[$typParam]($term.getObject(${idx})
              .asInstanceOf[_root_.java.util.List[$typParam]])
              .asScala
              .to[${coreType.typeConstructor}]
              """, coreType, None)
        }
      }

      // TODO: Refactor with above
      def classParameterMap(coreType: Type): (Int, Extractor, List[Builder]) = {
        val typParams = coreType.asInstanceOf[TypeRefApi].args
        (typParams.head, typParams.tail.head) match {
          case (keyTpe, valTpe)
            if !IsCaseClassImpl.isCaseClassType(c)(keyTpe) && IsCaseClassImpl.isCaseClassType(c)(valTpe) => {
            val innerList = TermName(c.freshName("innerList"))
            val cachedName = TermName(c.freshName("innerTuple"))
            val v = expandCaseClass(valTpe, 0, false, cachedName)
            val cachedResult = q"""
              val $innerList = _root_.scala.collection.JavaConverters.mapAsScalaMapConverter[$keyTpe, _root_.java.util.List[_root_.java.lang.Object]](
                $term.getObject($idx).asInstanceOf[_root_.java.util.Map[$keyTpe, _root_.java.util.List[_root_.java.lang.Object]]]
              ).asScala
              $innerList.map { i =>
                val $cachedName = new _root_.cascading.tuple.Tuple(
                  _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.lang.Object](i._2).asScala : _*)
                ..${v._3}
                (i._1, ${v._2})
              }.toMap
            """
            getPrimitive(cachedResult, coreType, None)
          }
          case (keyTpe, valTpe)
            if IsCaseClassImpl.isCaseClassType(c)(keyTpe) && !IsCaseClassImpl.isCaseClassType(c)(valTpe) => {
            val innerList = TermName(c.freshName("innerList"))
            val cachedName = TermName(c.freshName("innerTuple"))
            val v = expandCaseClass(keyTpe, 0, false, cachedName)
            val cachedResult = q"""
              val $innerList = _root_.scala.collection.JavaConverters.mapAsScalaMapConverter[_root_.java.util.List[_root_.java.lang.Object], $valTpe](
                $term.getObject($idx).asInstanceOf[_root_.java.util.Map[_root_.java.util.List[_root_.java.lang.Object], $valTpe]]
              ).asScala
              $innerList.map { i =>
                val $cachedName = new _root_.cascading.tuple.Tuple(
                  _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.lang.Object](i._1).asScala : _*)
                ..${v._3}
                (${v._2}, i._2)
              }.toMap
            """
            getPrimitive(cachedResult, coreType, None)
          }
          case (keyTpe, valTpe)
            if IsCaseClassImpl.isCaseClassType(c)(keyTpe) && IsCaseClassImpl.isCaseClassType(c)(valTpe) => {
            val innerList = TermName(c.freshName("innerList"))
            val keyCachedName = TermName(c.freshName("innerTuple"))
            val valCachedName = TermName(c.freshName("innerTuple"))
            val k = expandCaseClass(keyTpe, 0, false, keyCachedName)
            val v = expandCaseClass(valTpe, 0, false, valCachedName)
            val cachedResult = q"""
              val $innerList = _root_.scala.collection.JavaConverters.mapAsScalaMapConverter[_root_.java.util.List[_root_.java.lang.Object], _root_.java.util.List[_root_.java.lang.Object]](
                $term.getObject($idx).asInstanceOf[_root_.java.util.Map[_root_.java.util.List[_root_.java.lang.Object], _root_.java.util.List[_root_.java.lang.Object]]]
              ).asScala
              $innerList.map { i =>
                val $keyCachedName = new _root_.cascading.tuple.Tuple(
                  _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.lang.Object](i._1).asScala : _*)
                val $valCachedName = new _root_.cascading.tuple.Tuple(
                  _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.lang.Object](i._2).asScala : _*)
                ..${k._3}
                ..${v._3}
                (${k._2}, ${v._2})
              }.toMap
            """
            getPrimitive(cachedResult, coreType, None)
          }
          case (keyTpe, valTpe) => getPrimitive(
            q"""
              _root_.scala.collection.JavaConverters.mapAsScalaMapConverter[$keyTpe, $valTpe]($term.getObject(${idx})
                .asInstanceOf[_root_.java.util.Map[$keyTpe, $valTpe]])
                .asScala
                .toMap
              """, coreType, None)
        }
      }

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => getPrimitive(q"""$term.getString(${idx})""", typeOf[java.lang.String], None)
        case tpe if tpe =:= typeOf[Boolean] => getPrimitive(q"""$term.getBoolean(${idx})""", typeOf[java.lang.Boolean], Some(q"_root_.java.lang.Boolean.valueOf"))
        case tpe if tpe =:= typeOf[Short] => getPrimitive(q"""$term.getShort(${idx})""", typeOf[java.lang.Short], Some(q"_root_.java.lang.Short.valueOf"))
        case tpe if tpe =:= typeOf[Int] => getPrimitive(q"""$term.getInteger(${idx})""", typeOf[java.lang.Integer], Some(q"_root_.java.lang.Integer.valueOf"))
        case tpe if tpe =:= typeOf[Long] => getPrimitive(q"""$term.getLong(${idx})""", typeOf[java.lang.Long], Some(q"_root_.java.lang.Long.valueOf"))
        case tpe if tpe =:= typeOf[Float] => getPrimitive(q"""$term.getFloat(${idx})""", typeOf[java.lang.Float], Some(q"_root_.java.lang.Float.valueOf"))
        case tpe if tpe =:= typeOf[Double] => getPrimitive(q"""$term.getDouble(${idx})""", typeOf[java.lang.Double], Some(q"_root_.java.lang.Double.valueOf"))
        //case tpe if tpe =:= typeOf[java.sql.Timestamp] => getPrimitive(q"""t.getObject(${idx}).asInstanceOf[_root_.java.sql.Timestamp]""", typeOf[java.sql.Timestamp], None)
        // The TupleEntry has been created by Corc uses Java collections. We need to convert types, simple casting blows up.
        case tpe if tpe.erasure <:< weakTypeOf[Map[_,_]] => // Handle maps. Has to be a WeakTypeOf, otherwise no TypeTag found
          classParameterMap(tpe)
        case tpe if tpe.erasure <:< typeOf[TraversableOnce[Any]] =>
          // Handle any iterable as list. Arrays are a special case
          classParameter(tpe)
        case tpe if tpe.erasure <:< typeOf[Array[Any]] =>
          // Arrays are a special case, also handles an array of Bytes
          val typParam = tpe.asInstanceOf[TypeRefApi].args.head
          if (typParam =:= typeOf[Byte]) {
            getPrimitive(q"""t.getObject(${idx}).asInstanceOf[_root_.org.apache.hadoop.io.BytesWritable].getBytes""", tpe, None)
          } else {
            classParameter(tpe)
          }
        case tpe if tpe.erasure =:= typeOf[Option[Any]] && inOption =>
          c.abort(c.enclosingPosition, s"Nested options do not make sense being mapped onto a tuple fields in cascading.")

        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val innerType = tpe.asInstanceOf[TypeRefApi].args.head

          val (newIdx, extractor, builders) = matchField(innerType, idx, true, term)

          val cachedResult = newTermName(c.fresh(s"opti"))
          val extractorTypeVal: Tree = if (extractor.tpe =:= innerType)
            extractor.toTree
          else
            q"${innerType.typeSymbol.companionSymbol}.unbox($extractor)"

          val build = Builder(q"""
          val $cachedResult = if($extractor == null) {
              _root_.scala.Option.empty[$innerType]
            } else {
              _root_.scala.Some($extractorTypeVal)
            }
            """)
          (newIdx, Extractor(tpe, q"""$cachedResult"""), builders :+ build)
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => {
          val innerList = TermName(c.freshName("innerList"))
          val cachedName = TermName(c.freshName("innerTuple"))
          val v = expandCaseClass(tpe, 0, false, cachedName)
          val cachedResult = q"""
            val $innerList = _root_.scala.collection.JavaConverters.asScalaBufferConverter[_root_.java.lang.Object](
              $term.getObject($idx).asInstanceOf[_root_.java.util.List[_root_.java.lang.Object]]
            ).asScala
            val $cachedName = new _root_.cascading.tuple.Tuple($innerList : _*)
            ..${v._3}
            ${v._2}
            """

          //getPrimitive(q"""t.getObject(${idx}).asInstanceOf[$tpe]""", tpe, None)
          (idx + 1, Extractor(tpe, cachedResult), List[Builder]() )
        }
        case tpe if allowUnknownTypes => getPrimitive(q"""$term.getObject(${idx}).asInstanceOf[$tpe]""", tpe, None)
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes") // TODO: better error message
      }
    }

    def expandCaseClass(outerTpe: Type, parentIdx: Int, inOption: Boolean, term: TermName): (Int, Extractor, List[Builder]) = {
      val (idx, extractors, builders) = outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((parentIdx, List[Extractor](), List[Builder]())) {
        case ((idx, oldExtractors, oldBuilders), accessorMethod) =>
          val (newIdx, extractors, builders) = matchField(accessorMethod.returnType, idx, inOption, term)
          (newIdx, oldExtractors :+ extractors, oldBuilders ::: builders)
      }
      val cachedResult = newTermName(c.fresh(s"cacheVal"))

      val simpleBuilder = q"${outerTpe.typeSymbol.companionSymbol}(..$extractors)"
      val builder = if (inOption) {
        val tstOpt = extractors.map(e => q"$e == null").foldLeft(Option.empty[Tree]) {
          case (e, nxt) =>
            e match {
              case Some(t) => Some(q"$t || $nxt")
              case None => Some(nxt)
            }
        }
        tstOpt match {
          case Some(tst) =>
            q"""
              val $cachedResult: $outerTpe = if($tst) {
                null
              } else {
                $simpleBuilder
              }
            """
          case None => q"val $cachedResult = $simpleBuilder"
        }
      } else {
        q"val $cachedResult = $simpleBuilder"
      }
      (
        idx,
        Extractor(outerTpe, q"$cachedResult"),
        builders :+ Builder(builder))
    }

    val (finalIdx, extractor, builders) = expandCaseClass(T.tpe, 0, false, TermName("t"))
    if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")

    val res = q"""
    new _root_.com.twitter.scalding.TupleConverter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
     override def apply(t: _root_.cascading.tuple.TupleEntry): $T = {
        ..$builders
        $extractor
      }
      override val arity: scala.Int = ${finalIdx}
    }
    """

    c.Expr[TupleConverter[T]](res)
  }
}