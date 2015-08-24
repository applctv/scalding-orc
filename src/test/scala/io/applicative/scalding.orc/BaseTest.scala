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
