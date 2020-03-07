package org.mimirdb.caveats

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.catalyst.expressions._
import org.mimirdb.caveats._
import org.mimirdb.caveats.annotate.{
  AnnotationException,
  CaveatExistsInExpression,
  CaveatRangePlan
}
import org.mimirdb.caveats.enumerate.EnumeratePlanCaveats
import org.mimirdb.caveats.annotate.CaveatRangeStrategy

class ColumnImplicits(col: Column)
{
  def caveat(message: Column, family: String)(key: Column*): Column =
    new Column(ApplyCaveat(
      value = col.expr,
      message = message.cast(StringType).expr,
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

  def hasCaveat: Column =
    new Column(CaveatExistsInExpression(col.expr))
}

class DataFrameImplicits(df:DataFrame)
{
  def trackCaveats = Caveats.annotate(df)
  def rangeCaveats = Caveats.annotate(df, CaveatRangeStrategy())
  def listCaveatSets(
    row: Boolean = true,
    attributes: Set[String] = df.queryExecution
                                .analyzed
                                .output
                                .map { _.name }
                                .toSet
  ) = EnumeratePlanCaveats(df.queryExecution.analyzed)(
        row = row,
        attributes = attributes
      )
  def listCaveats(
    row: Boolean = true,
    attributes: Set[String] = df.queryExecution
                                .analyzed
                                .output
                                .map { _.name }
                                .toSet
  ) = listCaveatSets(row, attributes).flatMap { _.all(df.sparkSession) }

  def isAnnotated =
    df.queryExecution
      .analyzed
      .output
      .map { _.name }
      .exists { _.equals(Constants.ANNOTATION_ATTRIBUTE) }

  def assertAnnotated
  {
    if(!isAnnotated) throw new AnnotationException("You need to call df.trackCaveats first")
  }

  def caveats: Column =
  {
    assertAnnotated
    df(Constants.ANNOTATION_ATTRIBUTE)
  }
}

object implicits
{
  implicit def columnImplicits(col: Column): ColumnImplicits =
    new ColumnImplicits(col)
  implicit def dataFrameImplicits(df: DataFrame): DataFrameImplicits =
    new DataFrameImplicits(df)
}
