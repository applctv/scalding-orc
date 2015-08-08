package io.applicative.scalding.orc.impl

import java.sql.{Date, Timestamp}

import com.twitter.bijection.macros.impl.IsCaseClassImpl
import io.applicative.scalding.orc.SchemaWrapper
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo

import scala.collection.MapLike
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
        case tpe if tpe =:= typeOf[Char] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.charTypeInfo")
        case tpe if tpe =:= typeOf[Boolean] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo")
        case tpe if tpe =:= typeOf[Short] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo")
        case tpe if tpe =:= typeOf[Int] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo")
        case tpe if tpe =:= typeOf[Long] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo")
        case tpe if tpe =:= typeOf[Float] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo")
        case tpe if tpe =:= typeOf[Double] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo")
        case tpe if tpe =:= typeOf[Byte] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo")
        case tpe if tpe =:= typeOf[Timestamp] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo")
        case tpe if tpe =:= typeOf[Date] => simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo")

        case tpe if tpe.erasure =:= typeOf[Option[Any]] => // Handle options by extracting the type out of the option. FIXME: Extra handling at read time?
          val cacheName = TermName(c.freshName(s"optiIndx")) // TODO: see if needed
          val (newField, subTree) =
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, fieldName, q"$cacheName")

          (q"""$fieldName""", q"""$subTree""")
        case tpe if tpe.erasure <:< typeOf[Map[Any, Any]] => // Handle maps
          val cacheName = TermName(c.freshName(s"mapIndx"))
          val typParams = tpe.asInstanceOf[TypeRefApi].args
          val (_, keyTree) = matchField(typParams.head, fieldName, q"$cacheName")
          val (_, valTree) = matchField(typParams.tail.head, fieldName, q"$cacheName")
          simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo($keyTree, $valTree)")
        case tpe if tpe.erasure <:< typeOf[TraversableOnce[Any]] => // Handle any iterable as list
          val cacheName = TermName(c.freshName(s"listIndx"))
          val (_, elementTree) =
            matchField(tpe.asInstanceOf[TypeRefApi].args.head, fieldName, q"$cacheName")
          simpleType(q"_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo($elementTree)")
        case tpe if IsCaseClassImpl.isCaseClassType(c)(tpe) =>
          val caseClassExp = expandMethod(tpe, fieldName, pTree)
          val caseClassNames = caseClassExp.map(_._1)
          val caseClassTypes = caseClassExp.map(_._2)
          simpleType(q"""_root_.org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo(
            _root_.java.util.Arrays.asList($caseClassNames),
            _root_.java.util.Arrays.asList($caseClassTypes)
          )""")
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
    //val fields = set.map(_._1) // TODO: A cleaner way to get at this
    val fields = (0 until set.size).map(_.toString)
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
