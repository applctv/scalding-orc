package io.applicative.scalding.orc.impl

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import io.applicative.scalding.orc.SchemaWrapper
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo

import scala.language.experimental.macros

import scala.reflect.macros.Context

object SchemaTypeImpl {
  def caseClassSchemaType[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[SchemaWrapper[T]] = {
    import c.universe._

    if (!IsCaseClassImpl.isCaseClassType(c)(T.tpe))
      c.abort(c.enclosingPosition, s"""We cannot enforce ${T.tpe} is a case class, either it is not a case class or this macro call is possibly enclosed in a class.
        This will mean the macro is operating on a non-resolved type.""")

    def matchField(outerTpe: Type, fieldName: String, pTree: Tree): (Tree, Tree) = {
      def simpleType(accessor: Tree) =
        (q"""$fieldName""", q"""$accessor""")

      outerTpe match {
        case tpe if tpe =:= typeOf[String] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo")
        case tpe if tpe =:= typeOf[Boolean] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo")
        case tpe if tpe =:= typeOf[Short] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo")
        case tpe if tpe =:= typeOf[Int] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo")
        case tpe if tpe =:= typeOf[Long] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo")
        case tpe if tpe =:= typeOf[Float] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo")
        case tpe if tpe =:= typeOf[Double] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo")
//        case tpe if tpe.erasure =:= typeOf[Option[Any]] =>
//          val cacheName = newTermName(c.fresh(s"optiIndx"))
//          val (newIdx, subTree) =
//            matchField(tpe.asInstanceOf[TypeRefApi].args.head, idx, q"$cacheName")
//          val nullSetters = (idx until newIdx).map { curIdx =>
//            q"""tup.set($curIdx, null)"""
//          }
//
//          (newIdx, q"""
//            if($pTree.isDefined) {
//              val $cacheName = $pTree.get
//              $subTree
//            } else {
//              ..$nullSetters
//            }
//            """)

        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) => expandMethod(tpe, fieldName, pTree).head // FIXME
//        case tpe if allowUnknownTypes => simpleType(q"tup.set")
        case _ => c.abort(c.enclosingPosition, s"Case class ${T} is not pure primitives, Option of a primitive nested case classes")
      }
    }

    def expandMethod(outerTpe: Type, parentField: String, pTree: Tree): List[(Tree, Tree)] =
      outerTpe
        .declarations
        .collect { case m: MethodSymbol if m.isCaseAccessor => m }
//        .foldLeft((parentIdx, q"")) {
//        case ((idx, existingTree), accessorMethod) =>
//          val (newIdx, subTree) = matchField(accessorMethod.returnType, idx, q"""$pTree.$accessorMethod""")
//          (newIdx, q"""
//              $existingTree
//              $subTree""")
//      }
        .map { accessorMethod =>
          val fieldName = accessorMethod.name.toTermName.toString
          val fieldType = accessorMethod.returnType
          matchField(fieldType, fieldName, q"""$pTree.$accessorMethod""")
        }.toList

    val set = expandMethod(T.tpe, "", q"t")
    val fields = set.map(_._1) // TODO: A cleaner way to get at this
    val types = set.map(_._2)
    //if (finalIdx == 0) c.abort(c.enclosingPosition, "Didn't consume any elements in the tuple, possibly empty case class?")
    val res = q"""
    new _root_.io.applicative.scalding.orc.SchemaWrapper[$T] {
      val schema = _root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo(
        _root_.java.util.Arrays.asList(..$fields),
        _root_.java.util.Arrays.asList(..$types)
      ).asInstanceOf[_root_.org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo]
    }
    """
    c.Expr[SchemaWrapper[T]](res)
//    c.Expr[StructTypeInfo](null)
  }
}
