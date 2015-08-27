package io.applicative.scalding.orc

import java.io.File

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding._
import com.twitter.scalding.platform.HadoopPlatformJobTest
import MacroImplicits._

class ReadSampleTest extends BaseTest {
  "Read Sample" should {
    "read sample file" in {
      import TestValues._
      import scala.collection.JavaConverters._

      cluster.putFile(new File("src/test/resources/sample.orc"), "sample.orc")

      HadoopPlatformJobTest(new ReadSampleJob(_), cluster)
        .arg("output", "output1")
        .sink(TypedTsv[String]("output1")) { out =>
          out.size should be(2)
          out.head should be("hi")
          out.tail.head should be("bye")
        }.run
    }
  }
}

/*
* {"boolean1": true,
* "byte1": 100,
* "short1": 2048,
* "int1": 65536,
* "long1": 9223372036854775807,
* "float1": 2,
* "double1": -5,
 * "bytes1": [],
 * "string1": "bye",
 * "middle": {"list": [{"int1": 1, "string1": "bye"}, {"int1": 2, "string1": "sigh"}]},
 * "list": [{"int1": 100000000, "string1": "cat"}, {"int1": -100000, "string1": "in"}, {"int1": 1234, "string1": "hat"}],
 * "map": [{"key": "chani", "value": {"int1": 5, "string1": "chani"}}, {"key": "mauddib", "value": {"int1": 1, "string1": "mauddib"}}]}
* */
case class ReadSample(boolean1: Boolean, byte1: Byte, short1: Short, int1: Int, long1: Long,
                      float1: Float, double1: Double, bytes1: Array[Byte], string1: String,
                      middle: Middle, list: List[IntStr], map: Map[String, IntStr])
case class Middle(list: List[IntStr])
case class IntStr(int1: Int, string1: String)

class ReadSampleJob(args: Args) extends Job(args) {
  import MacroImplicits._

  val outputPath = args.required("output")

  TypedPipe
    .from(TypedOrc[ReadSample]("sample.orc"))
    .map(r => r.string1)
    .write(TypedTsv[String](outputPath))
}