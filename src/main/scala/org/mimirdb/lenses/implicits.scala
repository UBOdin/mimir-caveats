package org.mimirdb.lenses

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

class ColumnImplicits(col:Column)
{
  def caveatCast(t: String): Column   = 
    caveatCast(CatalystSqlParser.parseDataType(t))

  def caveatCast(t: DataType): Column =
    new Column(CaveatedCast(col.expr, t))

  def mergeWith(other:Column) =
    new Column(CaveatedMergeWith(col.expr, other.expr))
}

object implicits
{
  implicit def columnImplicits(col: Column): ColumnImplicits =
    new ColumnImplicits(col)
}