package io.applicative.scalding.orc

import com.twitter.scalding.{TupleConverter, TupleSetter}
import io.applicative.scalding.orc.impl._

import scala.language.experimental.macros

object MacroImplicits {
  implicit def materializeCaseClassSchemaType[T]: SchemaWrapper[T] = macro SchemaTypeImpl.caseClassSchemaType[T]
  implicit def materializeCaseClassTupleSetter[T]: TupleSetter[T] = macro TupleSetterImpl.caseClassTupleSetterImpl[T]
  implicit def materializeCaseClassTupleConverter[T]: TupleConverter[T] = macro TupleConverterImpl.caseClassTupleConverterImpl[T]
}
