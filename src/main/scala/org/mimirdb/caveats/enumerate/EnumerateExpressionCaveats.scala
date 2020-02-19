package org.mimirdb.caveats.enumerate

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import org.mimirdb.caveats._

object EnumerateExpressionCaveats
{
  def apply(
    plan: LogicalPlan, 
    expression: Expression, 
    condition: Expression
  ): Seq[CaveatSet] =
  {

    ExpressionDependency(expression){ condition => {
      case applyCaveat: ApplyCaveat => 
        applyCaveat.onPlan(Filter(condition, plan))
    }}
  }
}