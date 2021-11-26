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
  key: Seq[Expression] = Seq()
) extends Expression
    with UserDefinedExpression {

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = 
    CaveatRange(
      value = newChildren(0),
      lb = newChildren(1),
      ub = newChildren(2),
      message = newChildren(3),
      family = family,
      key = newChildren.drop(4).take(key.size),
    )

  override def name: String = 
    "CaveatRange"


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
    * Default message to use for caveats if the user has not provided any.
    *
    * @return
    */
  def defaultMessage = Literal("Caveated Value")

  /**
    * UDF that will be registered with Spark SQL to enable caveating from SQL.
    */
  def udf(children: Seq[Expression]): Expression =
    children match {
      case bg +: lb +: ub +: Nil => apply(bg, lb, ub, Literal(defaultMessage))
      case bg +: lb +: ub +: message +: Nil => apply(bg, lb, ub, message)
      case bg +: lb +: ub +: message +: key => apply(bg, lb, ub, message, None, key)
      case _ => throw new AnnotationException("""RangeCaveat needs to be provided with at least three inputs:
|- the expression calculating the value to be caveated
|- an expression calculating an lower bound on the value
|- an expression calculating an upper bound on the value
|
|Additionally, a message describing why the value was caveated can be provided. Finally, you may also provide one or more expressions calculating a key for identifying caveats of a particular type.
"""
      )
    }

  def registerUDF(s: SparkSession) = {
    s.sessionState
    .functionRegistry
    .createOrReplaceTempFunction(
      udfName, 
      udf _,
      "scala_udf"
    )
  }

}
