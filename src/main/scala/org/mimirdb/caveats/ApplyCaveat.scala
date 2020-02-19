package org.mimirdb.caveats

import org.apache.spark.sql.functions.array
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Project }
import org.apache.spark.sql.catalyst.InternalRow

import org.mimirdb.caveats.Constants._

case class ApplyCaveat(
  value: Expression,
  message: Expression,
  family: Option[String] = None,
  key: Seq[Expression] = Seq(),
  global: Boolean = false
) extends Expression
  with UserDefinedExpression
{
  def dataType = value.dataType
  protected def doGenCode(ctx: CodegenContext,ev: ExprCode): ExprCode = 
    value.genCode(ctx)
  def eval(input: InternalRow) = value.eval(input)
  def nullable = value.nullable
  def children = Seq(value, message) ++ key

  def onPlan(plan: LogicalPlan): CaveatSet =
  {
    if(global){ 
      val emptyRow = InternalRow()
      new SingletonCaveatSet(Caveat( 
        message = message.eval(emptyRow).asInstanceOf[String],
        family = family,
        key = key.map { _.eval(emptyRow) }
      ))
    } else {
      new EnumerableCaveatSet(
        Project(
          Seq(
            Alias(message, MESSAGE_ATTRIBUTE)(),
            Alias(CreateArray(key), KEY_ATTRIBUTE)()
          ), 
          plan
        ),
        family
      )
    }
  }
}


