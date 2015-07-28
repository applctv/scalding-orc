package io.applicative.scalding.orc

import cascading.scheme.Scheme
import cascading.tuple.Fields
import com.hotels.corc.cascading.{CascadingConverterFactory, OrcFile}
import com.twitter.bijection.macros.MacroGenerated
import com.twitter.scalding
import com.twitter.scalding.typed.TypedSink
import com.twitter.scalding._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoFactory}

import scala.language.experimental.macros

object TypedOrc {
  type MacroTupleConverter[T] = TupleConverter[T] with MacroGenerated

//  implicit def materializeCaseClassTupleSetter[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterWithUnknownImpl[T]
//  implicit def materializeCaseClassTupleConverter[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterWithUnknownImpl[T]

//  def apply[T](paths: Seq[String]): TypedOrcSource[T] =
//    new TypedFixedPathOrcTuple[T](paths)
  def apply[T](paths: Seq[String], schema: StructTypeInfo)(implicit converterImpl: MacroTupleConverter[T]): TypedOrcSource[T] =
    new TypedFixedPathOrcTuple[T](paths, schema, converterImpl)

  def apply[T](path: String)(implicit converterImpl: MacroTupleConverter[T]): TypedOrcSource[T] = apply[T](Seq(path), null: StructTypeInfo)

  /**
   * Create readable typed ORC source with filter predicate.
   */
  def apply[T](paths: Seq[String], fp: SearchArgument)(implicit converterImpl: MacroTupleConverter[T]): TypedOrcSource[T] =
    new TypedFixedPathOrcTuple[T](paths, null, converterImpl) {
      override def withFilter = Some(fp)
    }

  def apply[T](path: String, fp: SearchArgument)(implicit converterImpl: MacroTupleConverter[T]): TypedOrcSource[T] =
    apply[T](Seq(path), fp)
}

object TypedOrcSink {
  type MacroTupleConverter[T] = TupleConverter[T] with MacroGenerated
  type MacroTupleSetter[T] = TupleSetter[T] with MacroGenerated

//  def apply[T](paths: Seq[String]): TypedOrcSink[T] =
//    new TypedFixedPathOrcSink[T](paths)

  def apply[T](paths: Seq[String], schema: StructTypeInfo)
              (implicit converterImpl: MacroTupleConverter[T], setterImpl: MacroTupleSetter[T]): TypedOrcSink[T] =
    new TypedFixedPathOrcSink[T](paths, schema, converterImpl, setterImpl)

  def apply[T](path: String)(implicit converterImpl: MacroTupleConverter[T], setterImpl: MacroTupleSetter[T]): TypedOrcSink[T] = apply[T](Seq(path), null)
}

/**
 * Typed Orc tuple source/sink.
 */
trait TypedOrcSink[T] extends FileSource with Mappable[T]
  with TypedSink[T] {
  def schema: StructTypeInfo

  protected[this] implicit def converterImpl: TupleConverter[T] with MacroGenerated
  protected[this] implicit def setterImpl: TupleSetter[T] with MacroGenerated

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](converterImpl)

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](setterImpl)


  override def hdfsScheme = {
    val scheme = new OrcFile(Fields.size(schema.getAllStructFieldNames.size), schema, new CascadingConverterFactory)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

trait TypedOrcSource[T] extends FileSource with Mappable[T]
with TypedSource[T] with HasFilterPredicate {

  protected[this] implicit def converterImpl: TupleConverter[T] with MacroGenerated

  def schema: StructTypeInfo

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](converterImpl)


  override def hdfsScheme = {
    val builder = OrcFile.source()
    builder.schema(schema)
    val scheme = builder.build()
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

// TODO:
trait HasFilterPredicate {
  def withFilter: Option[SearchArgument] = None
}

class TypedFixedPathOrcTuple[T](val paths: Seq[String], val schema: StructTypeInfo, val converterImpl: TupleConverter[T] with MacroGenerated)
  extends FixedPathSource(paths: _*) with TypedOrcSource[T]
class TypedFixedPathOrcSink[T](val paths: Seq[String], val schema: StructTypeInfo, val converterImpl: TupleConverter[T] with MacroGenerated, val setterImpl: TupleSetter[T] with MacroGenerated)
  extends FixedPathSource(paths: _*) with TypedOrcSink[T]
