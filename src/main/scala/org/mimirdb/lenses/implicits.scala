package org.mimirdb.lenses

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.mimirdb.spark.sparkWorkarounds._

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

  def castWithCaveat(t: String, tzinfo: Option[String]): Column   = 
    castWithCaveat(CatalystSqlParser.parseDataType(t), tzinfo)

  def castWithCaveat(t: DataType, tzinfo: Option[String]): Column =
    castWithCaveat(t, null:String, tzinfo)

  def castWithCaveat(t: String, context: String, tzinfo: Option[String]): Column   = 
    castWithCaveat(CatalystSqlParser.parseDataType(t), context, tzinfo)

  def castWithCaveat(t: DataType, context: String, tzinfo: Option[String]): Column =
    new Column(CaveatedCast(col.expr, t, context = context, tzinfo = tzinfo))

  def shouldBeTheSameAs(other:Column) =
    new Column(CaveatedMerge(col.expr, other.expr))
}

class DataFrameImplicits(df:DataFrame)
{
  def deduplicateWithCaveats(keys: Seq[String], context: String = "") = 
  {
    val plan = 
      CaveatedDeduplicate(
        keys.map { df(_).expr }.map { 
          case a:Attribute => a
          case a => throw new RuntimeException(s"Expected $a to be an attribute")
        },
        df.queryExecution.analyzed,
        context
      )
    df.planToDF(plan)
  }
}

object implicits
{
  implicit def columnImplicits(col: Column): ColumnImplicits =
    new ColumnImplicits(col)
  implicit def dataFrameImplicits(df: DataFrame): DataFrameImplicits =
    new DataFrameImplicits(df)

}


