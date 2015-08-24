package io.applicative.scalding.orc

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{Job, Args}
import com.twitter.scalding.platform.HadoopPlatformJobTest
import MacroImplicits._

class SimpleWriteTest extends BaseTest {
  "Simple Write Job" should {
    "read and write simple values" in {
      import TestValues._
      import scala.collection.JavaConverters._

      HadoopPlatformJobTest(new WriteToTypedOrcTupleJobA(_), cluster)
        .arg("output", "output1")
        .sink(TypedOrc[SampleClassD](Seq("output1"))) { out =>
        val map = out.map(sampleClassD => (sampleClassD.x, sampleClassD.y)).toMap
        map(1) should equal("a")
        map(2) should equal("b")
      }.run
    }
  }
}
class WriteToTypedOrcTupleJobA(args: Args) extends Job(args) {
  import TypedOrc._
  import scala.collection.JavaConverters._
  val values = Seq(
    SampleClassD(1, "a"),
    SampleClassD(2, "b")
  )
  import MacroImplicits._

  val outputPath = args.required("output")

  val sink = TypedOrc[SampleClassD](Seq(outputPath))
  TypedPipe.from(values).write(sink)
}