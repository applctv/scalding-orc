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

import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding._
import com.twitter.scalding.platform.HadoopPlatformJobTest


class ColumnPruningTest extends BaseTest {
  "Column Pruning" should {
    "prune columns with partial schema" in {
      import TestValues._
      import scala.collection.JavaConverters._

      cluster.putFile(new File("src/test/resources/sample.orc"), "sample.orc")

      HadoopPlatformJobTest(new PruningSampleJob(_), cluster)
        .arg("output", "output1")
        .sink(TypedTsv[String]("output1")) { out =>
          out.size should be(2)
          out.head should be("hi")
          out.tail.head should be("bye")
        }.run
    }
  }
}

case class OneColumnFromSample(string1: String)

class PruningSampleJob(args: Args) extends Job(args) {
  import MacroImplicits._

  val outputPath = args.required("output")

  TypedPipe
    .from(TypedOrc[OneColumnFromSample]("sample.orc"))
    .map(r => r.string1)
    .write(TypedTsv[String](outputPath))
}