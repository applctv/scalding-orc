package io.applicative.scalding.orc

import com.twitter.scalding.{TupleConverter, TupleSetter}
import com.twitter.scalding.macros.impl.{TupleConverterImpl, TupleSetterImpl}
import io.applicative.scalding.orc.impl.SchemaTypeImpl
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo

import scala.language.experimental.macros

object MacroImplicits {
  implicit def materializeCaseClassSchemaType[T]: SchemaWrapper[T] = macro SchemaTypeImpl.caseClassSchemaType[T]
  implicit def materializeCaseClassTupleSetter[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterImpl[T]
  implicit def materializeCaseClassTupleConverter[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterImpl[T]
}
