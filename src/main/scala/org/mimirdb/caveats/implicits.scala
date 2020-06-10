package org.mimirdb.caveats

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats._
import org.mimirdb.caveats.annotate.{
  AnnotationException,
  CaveatExistsInExpression,
  CaveatRangePlan,
  CaveatRange,
  ApplyCaveatRange
}
import org.mimirdb.caveats.enumerate.EnumeratePlanCaveats
import org.mimirdb.caveats.annotate.CaveatExists
import org.mimirdb.caveats.annotate.CaveatRangeStrategy
import org.mimirdb.caveats.annotate.AnnotationInstrumentationStrategy

class ColumnImplicits(col: Column)
{
  def caveat(message: Column, family: String)(key: Column*): Column =
    caveatIf(message, family, lit(true))(key:_*)
  def caveat(message: Column): Column =
    caveatIf(message, lit(true))
  def caveat(message: String): Column =
    caveatIf(message, lit(true))

  def caveatIf(message: Column, family: String, condition: Column)(key: Column*): Column =
    new Column(ApplyCaveat(
      value = col.expr,
      message = message.cast(StringType).expr,
      family = Some(family),
      key = key.map { _.expr },
      condition = condition.expr
    ))
  def caveatIf(message: Column, condition: Column): Column =
    new Column(ApplyCaveat(
      value = col.expr,
      message = message.expr,
      condition = condition.expr
    ))
  def caveatIf(message: String, condition: Column): Column =
    caveatIf(lit(message), condition)

  def hasCaveat: Column =
    new Column(CaveatExistsInExpression(col.expr))

  def rangeCaveat(message: String, lb: Column, ub: Column): Column =
    rangeCaveat(lit(message), lb, ub)

  def rangeCaveat(message: Column, lb: Column, ub: Column): Column =
    new Column(ApplyCaveatRange(
      value = col.expr,
      lb = lb.expr,
      ub = ub.expr,
      message = message.expr)
    )

  def rangeCaveat(message: Column, family: String, lb: Column, ub: Column)(key: Column*): Column =
    new Column(ApplyCaveatRange(
      value = col.expr,
      lb = lb.expr,
      ub = ub.expr,
      message = message.cast(StringType).expr,
      family = Some(family),
      key = key.map { _.expr }
    ))

  def rangeCaveatIf(message: String, lb: Column, ub: Column, condition: Column): Column =
    rangeCaveatIf(lit(message), lb, ub, condition)

  def rangeCaveatIf(message: Column, lb: Column, ub: Column, condition: Column): Column =
    new Column(ApplyCaveatRange.cond(
      value = col.expr,
      lb = lb.expr,
      ub = ub.expr,
      message = message.expr,
      condition = condition.expr
    ))


  def rangeCaveatIf(message: Column, family: String, lb: Column, ub: Column, condition: Column)(key: Column*): Column = {
    new Column(ApplyCaveatRange.cond(
      value = col.expr,
      lb = lb.expr,
      ub = ub.expr,
      message = message.cast(StringType).expr,
      family = Some(family),
      key = key.map { _.expr },
      condition = condition.expr
    ))
  }

  def replaceAndRangeCaveat(message: Column, replacement: Column, lb: Column, ub: Column, condition: Column): Column = {
    new Column(CaseWhen(
      Seq(
        (
          condition.expr,
          ApplyCaveatRange(
            value = replacement.expr,
            lb = lb.expr,
            ub = ub.expr,
            message = message.cast(StringType).expr
          )
        )
      ),
      col.expr
    )
    )
  }

  def replaceAndRangeCaveat(message: Column, replacement: Column, lb: Column, ub: Column, condition: Column, family:String)(key: Column *): Column = {
    new Column(CaseWhen(
      Seq(
        (
          condition.expr,
          ApplyCaveatRange(
            value = replacement.expr,
            lb = lb.expr,
            ub = ub.expr,
            message = message.cast(StringType).expr,
            family = Some(family),
            key = key.map { _.expr },
          )
        )
      ),
      col.expr
    )
    )
  }

}

class DataFrameImplicits(df:DataFrame)
{
  def trackCaveats = Caveats.annotate(df, CaveatExists())
  def trackRangeCaveats = Caveats.annotate(df, CaveatRangeStrategy())
  def uncertainToAnnotation(
    model: UncertaintyModel,
    annotationType: AnnotationInstrumentationStrategy,
    trace: Boolean = false
  ) = Caveats.translateUncertainToAnnotation(df,
    model,
    annotationType,
    Constants.ANNOTATION_ATTRIBUTE,
    trace)

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


  val implicitTruth = new ColumnImplicits(lit(true))

  def caveat(message: Column): DataFrame =
    df.filter( implicitTruth.caveat(message) )
  def caveat(message: String): DataFrame =
    df.filter( implicitTruth.caveat(message) )
  def caveatIf(message: Column, condition: Column): DataFrame =
    df.filter( implicitTruth.caveatIf(message, condition) )
  def caveatIf(message: String, condition: Column): DataFrame =
    df.filter( implicitTruth.caveatIf(message, condition) )

  def stripCaveats: DataFrame =
    Caveats.strip(df)
}

object implicits
{
  implicit def columnImplicits(col: Column): ColumnImplicits =
    new ColumnImplicits(col)
  implicit def dataFrameImplicits(df: DataFrame): DataFrameImplicits =
    new DataFrameImplicits(df)
}
