package org.mimirdb.lenses

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

class ColumnImplicits(col:Column)
{
  def castWithCaveat(t: String): Column   = 
    castWithCaveat(CatalystSqlParser.parseDataType(t))

  def castWithCaveat(t: DataType): Column =
    castWithCaveat(t, null:String)

  def castWithCaveat(t: String, context: String): Column   = 
    castWithCaveat(CatalystSqlParser.parseDataType(t), context)

  def castWithCaveat(t: DataType, context: String): Column =
    new Column(CaveatedCast(col.expr, t, context = context))

  def shouldBeTheSameAs(other:Column) =
    new Column(CaveatedMerge(col.expr, other.expr))
}

object implicits
{
  implicit def columnImplicits(col: Column): ColumnImplicits =
    new ColumnImplicits(col)
}