package org.mimirdb.caveats.enumerate

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import org.mimirdb.caveats._
import com.typesafe.scalalogging.LazyLogging

object EnumerateExpressionCaveats
  extends LazyLogging
{
  def apply(
    plan: LogicalPlan, 
    expression: Expression, 
    vSlice: Expression,
    aggregates: AggregateInteraction.T = AggregateInteraction.IGNORE
  ): Seq[CaveatSet] =
  {
    val caveatSets = 
      ExpressionDependency(expression, vSlice, aggregates){ localVSlice => {
        case applyCaveat: ApplyCaveat => 
          applyCaveat.onPlan(Filter(localVSlice, plan))
      }}

    logger.trace(s"Explain Expression: $expression -> ${caveatSets.map{ _.toString }.mkString("; ")}")

    return caveatSets
  }
}