/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.applicative.scalding.orc

import java.io.File

import cascading.operation.DebugLevel
import cascading.tuple.Fields
import com.hotels.corc.cascading.SearchArgumentFactory
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding._
import com.twitter.scalding.platform.{LocalCluster, HadoopPlatformJobTest, HadoopPlatformTest}
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.scalatest.{Matchers, WordSpec}
import scala.language.experimental.macros

import MacroImplicits._

class TypedOrcTupleTest extends BaseTest {

  "TypedOrc" should {
    "read and write correctly" in {
      import TestValues._
      import MacroImplicits._

      def toMap[T](i: Iterable[T]): Map[T, Int] = i.groupBy(identity).mapValues(_.size)

      HadoopPlatformJobTest(new WriteToTypedOrcTupleJobB(_), cluster)
        .arg("output", "output1")
        .sink[SampleClassB](TypedOrc[SampleClassB](Seq("output1"))) {
        toMap(_) shouldBe toMap(values)
      }.run

      HadoopPlatformJobTest(new ReadWithFilterPredicateJob(_), cluster)
        .arg("input", "output1")
        .arg("output", "output2")
        .sink[Boolean]("output2") { toMap(_) shouldBe toMap(values.filter(_.string == "B1").map(_.a.bool)) }
        .run
    }
  }
}

object TestValues {
  val values = Seq(
    SampleClassB("B1", Some(4.0D), SampleClassA(bool = true, 5, 1L, 1.2F, 1), List(1, 2),
      List(SampleClassD(1, "1"), SampleClassD(2, "2")), Set(1D, 2D), Set(SampleClassF(1, 1F)), Map(1 -> "foo")),
    SampleClassB("B2", Some(3.0D), SampleClassA(bool = false, 4, 2L, 2.3F, 2), List(3, 4), Nil, Set(3, 4), Set(),
      Map(2 -> "bar"), Map(SampleClassD(0, "z") -> SampleClassF(0, 3), SampleClassD(0, "y") -> SampleClassF(2, 6))),
    SampleClassB("B3", None, SampleClassA(bool = true, 6, 3L, 3.4F, 3), List(5, 6),
      List(SampleClassD(3, "3"), SampleClassD(4, "4")), Set(5, 6), Set(SampleClassF(2, 2F))),
    SampleClassB("B4", Some(5.0D), SampleClassA(bool = false, 7, 4L, 4.5F, 4), Nil,
      List(SampleClassD(5, "5"), SampleClassD(6, "6")), Set(), Set(SampleClassF(3, 3F), SampleClassF(5, 4F)),
      Map(3 -> "foo2"), Map(SampleClassD(0, "q") -> SampleClassF(4, 3))))
}

case class SampleClassA(bool: Boolean, short: Short, long: Long, float: Float, byte: Byte)

case class SampleClassB(string: String, double: Option[Double], a: SampleClassA, intList: List[Int],
                        dList: List[SampleClassD], doubleSet: Set[Double], fSet: Set[SampleClassF], intStringMap: Map[Int, String] = Map(),
                        dfMap: Map[SampleClassD, SampleClassF] = Map())

case class SampleClassC(string: String, a: SampleClassA)
case class SampleClassD(x: Int, y: String)
case class SampleClassF(w: Byte, z: Float)

class WriteToTypedOrcTupleJobB(args: Args) extends Job(args) {
  import TestValues._
  import MacroImplicits._

  val outputPath = args.required("output")

  val sink = TypedOrc[SampleClassB](outputPath)
  TypedPipe.from(values).write(sink)
}

class ReadWithFilterPredicateJob(args: Args) extends Job(args) {
  import MacroImplicits._
  val fp: SearchArgument = SearchArgumentFactory.newBuilder()
    .startAnd()
    .equals(new Fields("string", classOf[String]), "B1")
    .end()
    .build()

  val inputPath = args.required("input")
  val outputPath = args.required("output")

  val input = TypedOrc[SampleClassC](inputPath, fp)

  TypedPipe.from(input).map(_.a.bool).write(TypedTsv[Boolean](outputPath))
}