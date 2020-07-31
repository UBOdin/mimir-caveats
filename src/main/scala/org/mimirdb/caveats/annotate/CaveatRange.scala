package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession

/**
  * Expression that annotates an
  * [[org.apache.spark.sql.catalyst.expressions.Expression]] with a lower bound
  * and an upper bound on its value across all possible worlds. Under normal evaluation
  * of expressions, caveats evaluate to the result of value. When the expression is rewritten
  * for tracking uncertainty, the bounds are used directly.
  *
  *  @constructor wraps an expression value to encode bounds on its possible value
  *  @param value the wrapped expression
  *  @param lb expression calculating the lower bound
  *  @param ub expression calculating the upper bound
  *  @param message expression calculating the message decribing the reason why this expression is uncertain and
  *  @param family enables grouping of caveats
  *  @param key expressions calcualting an  identifier for caveats of a family within the context of a query
  */
case class CaveatRange(
  value: Expression,
  lb: Expression,
  ub: Expression,
  message: Expression,
  family: Option[String] = None,
  key: Seq[Expression] = Seq()) extends Expression
    with UserDefinedExpression {



  def dataType = value.dataType
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    value.genCode(ctx) // if no instrumentation this is just a value

  def eval(input: InternalRow) = value.eval(input)

  def rangeEval(input: InternalRow) = (lb.eval(input), value.eval(input), ub.eval(input))

  def nullable = value.nullable

  def children = Seq(value, lb, ub, message) ++ key
}

object ApplyCaveatRange {

  /**
    * Apply caveat to an expression if a certain condition holds.
    */
  def cond(
    value: Expression,
    lb: Expression,
    ub: Expression,
    message: Expression,
    condition: Expression,
    family: Option[String] = None,
    key: Seq[Expression] = Seq(),
  ): Expression =
    CaseWhen(
      Seq((condition,CaveatRange(value, lb, ub, message, family, key))),
      value)

  def apply(
    value: Expression,
    lb: Expression,
    ub: Expression,
    message: Expression,
    family: Option[String] = None,
    key: Seq[Expression] = Seq(),
  ): Expression =
    CaveatRange(value, lb, ub, message, family, key)

  def udfName = "RangeCaveat"

  /**
    * UDF that will be registered with Spark SQL to enable caveating from SQL
    */
  def udf(bg: Any, lb: Any, ub: Any): String = // cannot register UDFs that return Any. We never evaluate this function anyways, so just pretend it returns a string
  {
    bg.toString()
  }

  def registerUDF(s: SparkSession) = {
    s.udf.register(udfName, udf _)
  }

}
