package org.mimirdb.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.BoundReference

object SchemaLookup
{
  def rowReferences(row: Row): Seq[(String,BoundReference)] = 
    row.schema.fields.zipWithIndex.map { case (field, idx) =>
      field.name -> BoundReference(idx, field.dataType, true)
    }
}