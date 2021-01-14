package org.mimirdb.lenses

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{ DataType, StringType }
import org.mimirdb.caveats.ApplyCaveat
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Aggregate }

object CaveatedDeduplicate
  extends LazyLogging
{
  def apply(
    keys: Seq[Attribute],
    plan: LogicalPlan,
    context: String = null, 
    family: Option[String] = None,
    key: Seq[Expression] = Seq()
  ): LogicalPlan = {
    val isKey = keys.map { _.exprId }.toSet
    val nonKeyAttrs = plan.output.filter { attr => !isKey(attr.exprId) }
    val nonKeyAttrCounts = 
      nonKeyAttrs


    Aggregate(keys,
      keys ++ 
      nonKeyAttrs.map { attr =>
        Alias(
          ApplyCaveat(
            First(attr, true)
              .toAggregateExpression(false),
            Literal(s"Multiple Values for ${attr.name}${if(context != null){ " "+context } else { "" }}"),
            family = family,
            key = key,
            global = false,
            condition = GreaterThan(Count(Seq(attr)).toAggregateExpression(true), Literal(1))
          ),
          attr.name
        )(attr.exprId)
      },
      plan
    )
  }
}