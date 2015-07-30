package io.applicative.scalding.orc

import cascading.scheme.Scheme
import cascading.tuple.Fields
import com.hotels.corc.cascading.{CascadingConverterFactory, OrcFile}
import com.twitter.scalding.typed.TypedSink
import com.twitter.scalding._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, StructTypeInfo, TypeInfoFactory}

import scala.language.experimental.macros

import MacroImplicits._

object TypedOrc {

  def apply[T](paths: Seq[String])(implicit converterImpl: TupleConverter[T], schema: SchemaWrapper[T]): TypedOrcSource[T] =
    new TypedFixedPathOrcTuple[T](paths, schema, converterImpl)

  def apply[T](path: String)(implicit converterImpl: TupleConverter[T], schema: SchemaWrapper[T]): TypedOrcSource[T] = apply[T](Seq(path))

  /**
   * Create readable typed ORC source with filter predicate.
   */
  def apply[T](paths: Seq[String], fp: SearchArgument)(implicit converterImpl: TupleConverter[T]): TypedOrcSource[T] =
    new TypedFixedPathOrcTuple[T](paths, null, converterImpl) {
      override def withFilter = Some(fp)
    }

  def apply[T](path: String, fp: SearchArgument)(implicit converterImpl: TupleConverter[T]): TypedOrcSource[T] =
    apply[T](Seq(path), fp)
}

object TypedOrcSink {

  def apply[T](paths: Seq[String])
              (implicit converterImpl: TupleConverter[T], setterImpl: TupleSetter[T], schema: SchemaWrapper[T]): TypedOrcSink[T] =
    new TypedFixedPathOrcSink[T](paths, schema, converterImpl, setterImpl)

  def apply[T](path: String)(implicit converterImpl: TupleConverter[T], setterImpl: TupleSetter[T], schema: SchemaWrapper[T]): TypedOrcSink[T] = apply[T](Seq(path))
}

/**
 * Typed Orc tuple source/sink.
 */
trait TypedOrcSink[T] extends FileSource with Mappable[T]
  with TypedSink[T] {
  def schema: SchemaWrapper[T]

  def converterImpl: TupleConverter[T]
  def setterImpl: TupleSetter[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](converterImpl)

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](setterImpl)


  override def hdfsScheme = {
    import scala.collection.JavaConverters._
    //val scheme = new OrcFile(Fields.size(schema.getAllStructFieldNames.size), schema, new CascadingConverterFactory)
//    val names = schema.getAllStructFieldNames.asScala
//    val fields = new Fields(names: _*)
    val fields = Fields.size(schema.get.getAllStructFieldNames.size)
    val scheme = new OrcFile(fields, schema.get, new CascadingConverterFactory)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

trait TypedOrcSource[T] extends FileSource with Mappable[T]
with TypedSource[T] with HasFilterPredicate {

  def converterImpl: TupleConverter[T]// with MacroGenerated

  def schema: SchemaWrapper[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](converterImpl)


  override def hdfsScheme = {
    // FIXME: First column is projection
    val scheme = new OrcFile(schema.get, withFilter.orNull, Fields.size(schema.get.getAllStructFieldNames.size), schema.get,
      new CascadingConverterFactory)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

// TODO:
trait HasFilterPredicate {
  def withFilter: Option[SearchArgument] = None
}

class TypedFixedPathOrcTuple[T](val paths: Seq[String], val schema: SchemaWrapper[T], val converterImpl: TupleConverter[T])
  extends FixedPathSource(paths: _*) with TypedOrcSource[T]
class TypedFixedPathOrcSink[T](val paths: Seq[String], val schema: SchemaWrapper[T], val converterImpl: TupleConverter[T], val setterImpl: TupleSetter[T])
  extends FixedPathSource(paths: _*) with TypedOrcSink[T]
