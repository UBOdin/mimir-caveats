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
    vSlice: Expression,
    aggregates: AggregateInteraction.T = AggregateInteraction.IGNORE
  ): Seq[CaveatSet] =
  {

    ExpressionDependency(expression, vSlice, aggregates){ localVSlice => {
      case applyCaveat: ApplyCaveat => 
        applyCaveat.onPlan(Filter(localVSlice, plan))
    }}
  }
}