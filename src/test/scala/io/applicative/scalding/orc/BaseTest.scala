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

import com.twitter.scalding.platform.HadoopPlatformTest
import org.scalatest.{Matchers, WordSpec}

abstract class BaseTest  extends WordSpec with Matchers with HadoopPlatformTest {
  override def initialize() = {
    val temp = cluster.initialize()
    cluster.addClassSourceToClassPath(classOf[com.twitter.bijection.Injection[_, _]])
    cluster.addClassSourceToClassPath(classOf[com.twitter.bijection.macros.MacroGenerated])
    cluster.addClassSourceToClassPath(classOf[com.twitter.scalding.serialization.Boxed[_]])
    cluster.addClassSourceToClassPath(classOf[com.hotels.corc.mapred.CorcOutputFormat])
    cluster.addClassSourceToClassPath(classOf[com.hotels.corc.cascading.OrcFile])
    cluster.addClassSourceToClassPath(classOf[com.hotels.corc.Corc])
    cluster.addClassSourceToClassPath(classOf[org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo])
    cluster.addClassSourceToClassPath(classOf[org.apache.hadoop.hdfs.DFSClient])
    temp
  }
}
