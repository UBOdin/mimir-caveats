package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.InternalRow

case class CaveatRange(
  value: Expression,
  lb: Expression,
  ub: Expression,
  message: Expression,
  family: Option[String] = None,
  key: Seq[Expression] = Seq()
) extends Expression
  with UserDefinedExpression
{
  def dataType = value.dataType
  protected def doGenCode(ctx: CodegenContext,ev: ExprCode): ExprCode =
    value.genCode(ctx) // if no instrumentation this is just a value
  def eval(input: InternalRow) = value.eval(input)
  def rangeEval(input: InternalRow) = (lb.eval(input), value.eval(input), ub.eval(input))
  def nullable = value.nullable
  def children = Seq(value, lb, ub, message) ++ key
}
