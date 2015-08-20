package io.applicative.scalding.orc

import cascading.scheme.Scheme
import cascading.tuple.Fields
import com.hotels.corc.cascading.{CascadingConverterFactory, OrcFile}
import com.twitter.bijection.macros.MacroGenerated
import com.twitter.scalding.typed.{TypedSource, TypedSink}
import com.twitter.scalding._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument

import scala.language.experimental.macros

import MacroImplicits._

object TypedOrc {

  def apply[T](paths: Seq[String])
              (implicit converterImpl: TupleConverter[T],
               setterImpl: TupleSetter[T],
               schema: SchemaWrapper[T]): TypedOrc[T] =
    new TypedFixedPathOrcTuple[T](paths, schema, converterImpl, setterImpl)

  def apply[T](path: String)
              (implicit converterImpl: TupleConverter[T],
               setterImpl: TupleSetter[T],
               schema: SchemaWrapper[T]): TypedOrc[T] = apply[T](Seq(path))

  /**
   * Create readable typed ORC source with filter predicate.
   */
  //FIXME
//  def apply[T](paths: Seq[String], fp: SearchArgument)
//              (implicit converterImpl: TupleConverter[T], setterImpl: TupleSetter[T]): TypedOrc[T] =
//    new TypedFixedPathOrcTuple[T](paths, null, converterImpl) {
//      override def withFilter = Some(fp)
//    }

  // FIXME
//  def apply[T](path: String, fp: SearchArgument)
//              (implicit converterImpl: TupleConverter[T]): TypedOrc[T] =
//    apply[T](Seq(path), fp)
}

/*object TypedOrcSink {

  def apply[T](paths: Seq[String])
              (implicit converterImpl: TupleConverter[T],
               setterImpl: TupleSetter[T],
               schema: SchemaWrapper[T]): TypedOrcSink[T] =
    new TypedFixedPathOrcSink[T](paths, schema, converterImpl, setterImpl)

  def apply[T](path: String)
              (implicit converterImpl: TupleConverter[T],
               setterImpl: TupleSetter[T],
               schema: SchemaWrapper[T]): TypedOrcSink[T] = apply[T](Seq(path))
}*/

/**
 * Typed Orc tuple source/sink.
 */
trait TypedOrc[T] extends FileSource with Mappable[T]
  with TypedSink[T] with TypedSource[T] with HasFilterPredicate {
  import scala.collection.JavaConverters._
  def schema: SchemaWrapper[T]

  def converterImpl: TupleConverter[T]
  def setterImpl: TupleSetter[T]

  //val fields = Fields.size(schema.get.getAllStructFieldNames.size)
  //val fields = new Fields(schema.get.getAllStructFieldNames.asScala: _*)
  val fields = new Fields(Fields.names(schema.get.getAllStructFieldNames.asScala: _*): _*)
  var scheme: OrcFile = _

  def readScheme = new OrcFile(schema.get, withFilter.orNull, fields, /*FIXME: Null to read schema from file. Required for column pruning*/schema.get, new CascadingConverterFactory)
  def writeScheme = new OrcFile(fields, schema.get, new CascadingConverterFactory)

  override def sinkFields: Fields = fields

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](converterImpl)

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](setterImpl)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode) = {
    readOrWrite match {
      case Read => scheme = readScheme
      case Write => scheme = writeScheme
    }

    if (!converter.getClass.getInterfaces.contains(classOf[MacroGenerated])) {
      throw new Exception("Failed to generate proper converter - check implicits") // TODO: Better message
    }
    if (!setter.getClass.getInterfaces.contains(classOf[MacroGenerated])) {
      throw new Exception("Failed to generate proper setter - check implicits") // TODO: Better message
    }

    super.createTap(readOrWrite)(mode)
  }

  override def hdfsScheme = HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])

//  override def hdfsScheme = {
//    import scala.collection.JavaConverters._
//    //val scheme = new OrcFile(Fields.size(schema.getAllStructFieldNames.size), schema, new CascadingConverterFactory)
////    val names = schema.getAllStructFieldNames.asScala
////    val fields = new Fields(names: _*)
//
//    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
//  }
}

/*
 * Trying to unify the two

trait TypedOrcSource[T] extends FileSource with Mappable[T]
with TypedSource[T] with HasFilterPredicate {

  import scala.collection.JavaConverters._
  def converterImpl: TupleConverter[T]// with MacroGenerated

  def schema: SchemaWrapper[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](converterImpl)

//  val names = schema.get.getAllStructFieldNames.asScala
//  val fields = new Fields(names: _*)
  val fields = Fields.size(schema.get.getAllStructFieldNames.size)
  val scheme = new OrcFile(schema.get, withFilter.orNull, fields, schema.get, new CascadingConverterFactory)



  override def hdfsScheme = {
    import scala.collection.JavaConverters._
    // FIXME: First column is projection
//        val names = schema.get.getAllStructFieldNames.asScala
//        val fields = new Fields(names: _*)
//    val scheme = new OrcFile(schema.get, withFilter.orNull, Fields.size(schema.get.getAllStructFieldNames.size), schema.get,
//    //val scheme = new OrcFile(schema.get, withFilter.orNull, fields, schema.get,
//      new CascadingConverterFactory)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}*/

// TODO:
trait HasFilterPredicate {
  def withFilter: Option[SearchArgument] = None
}

//class TypedFixedPathOrcTuple[T](val paths: Seq[String], val schema: SchemaWrapper[T], val converterImpl: TupleConverter[T])
//  extends FixedPathSource(paths: _*) with TypedOrc[T]
class TypedFixedPathOrcTuple[T](val paths: Seq[String], val schema: SchemaWrapper[T], val converterImpl: TupleConverter[T], val setterImpl: TupleSetter[T])
  extends FixedPathSource(paths: _*) with TypedOrc[T]
