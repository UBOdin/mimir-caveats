package org.mimirdb.caveats

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.catalyst.expressions._

class ColumnImplicits(col: Column)
{
  def caveat(message: Column, family: String)(key: Column*): Column =
    new Column(ApplyCaveat(
      value = col.expr,
      message = message.expr,
      family = Some(family),
      key = key.map { _.expr }
    ))

  def caveat(message: Column): Column =
    new Column(ApplyCaveat(
      value = col.expr,
      message = message.expr
    ))

  def caveat(message: String): Column =
    new Column(ApplyCaveat(
      value = col.expr,
      message = Literal(message)
    ))
}

class DataFrameImplicits(df:DataFrame)
{
  def annotate = Caveats.annotate(df)
}

object implicits
{
  implicit def columnImplicits(col: Column): ColumnImplicits = 
    new ColumnImplicits(col)
  implicit def dataFrameImplicits(df: DataFrame): DataFrameImplicits = 
    new DataFrameImplicits(df)
}