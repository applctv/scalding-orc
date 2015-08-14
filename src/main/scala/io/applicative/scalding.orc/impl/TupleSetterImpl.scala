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
object TupleSetterImpl {
  def caseClassTupleSetterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._

    val allowUnknownTypes: Boolean = true

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(outerTpe: Type, idx: Int, pTree: Tree, name: TermName): (Int, Tree) = {
      def simpleType(accessor: Tree) =
        (idx + 1, q"""${accessor}(${idx}, $pTree)""")

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => simpleType(q"$name.setString")
        case tpe if tpe =:= typeOf[Boolean] => simpleType(q"$name.setBoolean")
        case tpe if tpe =:= typeOf[Short] => simpleType(q"$name.setShort")
        case tpe if tpe =:= typeOf[Int] => simpleType(q"$name.setInteger")
        case tpe if tpe =:= typeOf[Long] => simpleType(q"$name.setLong")
        case tpe if tpe =:= typeOf[Float] => simpleType(q"$name.setFloat")
        case tpe if tpe =:= typeOf[Double] => simpleType(q"$name.setDouble")
        case tpe if tpe =:= typeOf[java.sql.Timestamp] => simpleType(q"$name.set")
        case tpe if tpe.erasure <:< weakTypeOf[Map[_,_]] => // Handle maps. Has to be a WeakTypeOf, otherwise no TypeTag found
          //FIXME: if type parameters are case classes, setting fails. need to convert them to lists
          val typParams = tpe.asInstanceOf[TypeRefApi].args
          val keyParam = typParams.head
          val valueParam = typParams.tail.head
          (idx + 1,
            q"""$name.set(${idx}, _root_.scala.collection.JavaConverters.mapAsJavaMapConverter[$keyParam, $valueParam]($pTree).asJava)"""
            )
        case tpe if tpe.erasure <:< typeOf[TraversableOnce[Any]] =>
          // Handle any iterable as list. Arrays are a special case
          val typParam = tpe.asInstanceOf[TypeRefApi].args.head
          (idx + 1,
            q"""$name.set(${idx}, _root_.scala.collection.JavaConverters.seqAsJavaListConverter[$typParam]($pTree.toSeq).asJava)"""
            )
        case tpe if tpe.erasure <:< typeOf[Array[Any]] =>
          // Arrays are a special case, also handles an array of Bytes
          val typParam = tpe.asInstanceOf[TypeRefApi].args.head
          if (typParam =:= typeOf[Byte]) {
            (idx + 1,
              q"""$name.set(${idx}, new _root_.org.apache.hadoop.io.BytesWritable($pTree))"""
              )
          } else {
            (idx + 1,
              q"""$name.set(${idx}, _root_.scala.collection.JavaConverters.seqAsJavaListConverter[$typParam]($pTree.toSeq).asJava)"""
              )
          }
        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
          val cacheName = TermName(c.fresh(s"optiIndx"))
          val (newIdx, subTree) =
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, idx, q"$cacheName", name)
          val nullSetters = (idx until newIdx).map { curIdx =>
            q"""$name.set($curIdx, null)"""
          }

          (newIdx, q"""
            if($pTree.isDefined) {
              val $cacheName = $pTree.get
              $subTree
            } else {
              ..$nullSetters
            }
            """)

        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => {
          val cacheName = TermName(c.freshName(s"innerTuple"))
          val (innerIndex, iTree) = expandMethod(tpe, 0, pTree, cacheName)
          (idx + 1,
            q"""
               val $cacheName = _root_.cascading.tuple.Tuple.size($innerIndex)
               $iTree
               $name.set(${idx}, _root_.cascading.tuple.Tuple.elements($cacheName))"""
            )
        }
        case tpe if allowUnknownTypes => simpleType(q"$name.set")
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, parentIdx: Int, pTree: Tree, name: TermName): (Int, Tree) =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
        .foldLeft((parentIdx, q"")) {
        case ((idx, existingTree), accessorMethod) =>
          val (newIdx, subTree) = matchField(accessorMethod.returnType, idx, q"""$pTree.$accessorMethod""", name)
          (newIdx, q"""
              $existingTree
              $subTree""")
      }

    val (finalIdx, set) = expandMethod(T.tpe, 0, q"t", TermName("tup"))
    if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")
    val res = q"""
    new _root_.com.twitter.scalding.TupleSetter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val tup = _root_.cascading.tuple.Tuple.size($finalIdx)
        $set
        tup
      }
      override val arity: _root_.scala.Int = $finalIdx
    }
    """
    c.Expr[TupleSetter[T]](res)
  }
}
