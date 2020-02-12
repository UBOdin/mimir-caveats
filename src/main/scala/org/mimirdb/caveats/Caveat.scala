package org.mimirdb.caveats

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.InternalRow

case class Caveat(
  value: Expression,
  message: Expression,
  family: Option[String] = None,
  key: Seq[Expression] = Seq()
) extends Expression
  with UserDefinedExpression
{
  def dataType = value.dataType
  protected def doGenCode(ctx: CodegenContext,ev: ExprCode): ExprCode = 
    value.genCode(ctx)
  def eval(input: InternalRow) = value.eval(input)
  def nullable = value.nullable
  def children = Seq(value, message) ++ key
}


