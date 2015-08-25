package io.applicative.scalding.orc

import com.twitter.scalding._
import io.applicative.scalding.orc.impl.SchemaTypeImpl
import scala.language.experimental.macros

class SampleJob(args: Args) extends Job(args) {
//  val values = Seq(
//    SampleClassD(1, "a"),
//    SampleClassD(2, "b")
//  )
//  import MacroImplicits._
//
//  val outputPath = "target/test"

//  TypedPipe
//    .from(values)
//    .write(TypedOrcSink[SampleClassD](outputPath)(materializeCaseClassTupleConverter[SampleClassD], materializeCaseClassTupleSetter[SampleClassD], materializeCaseClassSchemaType[SampleClassD]))
}

//case class SampleClassD(a: Int, b: String)
