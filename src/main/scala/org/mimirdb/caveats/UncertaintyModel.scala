package org.mimirdb.caveats

import org.apache.spark.sql.types.StructType

trait UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType
}

case class TupleIndependentProbabilisticDatabase(probabilityAttr: String)
extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(_.name == probabilityAttr))
  }
}

case class XDB(xtupIdAttr: String, probabilityAttr: String)
extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(x => (x.name == xtupIdAttr) || (x.name == probabilityAttr)))
  }
}

object VTable
extends UncertaintyModel {
  def adaptedSchema(schema: StructType): StructType = {
    schema
  }
}
