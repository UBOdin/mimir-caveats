package org.mimirdb.caveats

import org.apache.spark.sql.functions.array
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Project, Filter }
import org.apache.spark.sql.catalyst.InternalRow

import org.mimirdb.caveats.Constants._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.BooleanType

case class ApplyCaveat(
  value: Expression,
  message: Expression,
  family: Option[String] = None,
  key: Seq[Expression] = Seq(),
  global: Boolean = false,
  condition: Expression = Literal(true)
) extends Expression
  with UserDefinedExpression
{
  def dataType = value.dataType
  protected def doGenCode(ctx: CodegenContext,ev: ExprCode): ExprCode =
    value.genCode(ctx)
  def eval(input: InternalRow) = value.eval(input)
  def nullable = value.nullable
  def children = Seq(value, message) ++ key :+ condition

  def onPlan(plan: LogicalPlan): CaveatSet =
  {
    if(global){
      val emptyRow = InternalRow()
      new SingletonCaveatSet(Caveat(
        message = message.eval(emptyRow).asInstanceOf[String],
        family = family,
        key = key.map { _.eval(emptyRow) }.map { Literal(_) }
      ))
    } else {
      new EnumerableCaveatSet(
        Project(
          Seq(
            Alias(message, MESSAGE_ATTRIBUTE)(),
            Alias(CreateArray(key), KEY_ATTRIBUTE)()
          ),
          Filter(
            condition,
            plan
          )
        ),
        family
      )
    }
  }
}

object ApplyCaveat {

  def replace(
    value: Expression,
    replacement: Expression,
    condition: Expression,
    message: Expression,
    key: Seq[Expression] = Seq()
  ): Expression =
    CaseWhen(
      Seq(
        (
          condition,
          ApplyCaveat(
            value = replacement,
            message = message,
            key = key
          )
        )
      ),
      value
    )

}

case class HasCaveat(
  value: Expression)
    extends Expression
    with UserDefinedExpression
{

  override def children: Seq[Expression] = Seq(value)

  override def nullable: Boolean = value.nullable

  override def eval(input: InternalRow): Any = false

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    Literal(false).genCode(ctx)

  override def dataType: DataType = BooleanType
}
