package io.applicative.scalding.orc

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo

trait SchemaWrapper[T] {
  def schema: StructTypeInfo
  def get = schema
}
