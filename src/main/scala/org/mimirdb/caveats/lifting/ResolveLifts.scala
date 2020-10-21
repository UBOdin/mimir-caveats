package org.mimirdb.caveats.lifting

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.mimirdb.caveats.{ ApplyCaveat, HasCaveat }
import org.mimirdb.caveats.annotate.{ 
  CaveatExistsInPlan,
  CaveatExistsInExpression
}
import org.mimirdb.spark.sparkWorkarounds._


object ResolveLifts
{
  def apply(
    df: DataFrame,
    pedantic: Boolean = true,
    ignoreUnsupported: Boolean = false,
    trace: Boolean = false
  ): DataFrame = 
    df.extend(apply(
      df.plan,
      pedantic,
      ignoreUnsupported,
      trace
    ))

  def apply(
    plan: LogicalPlan,
    pedantic: Boolean,
    ignoreUnsupported: Boolean,
    trace: Boolean
  ): LogicalPlan = 
    rewrite(
      plan, 
      new CaveatExistsInPlan(pedantic, ignoreUnsupported, trace)
    )

  def rewrite(
    plan: LogicalPlan, 
    caveats: CaveatExistsInPlan
  ): LogicalPlan =
    plan.transformUp {
      case original@Filter(condition, child) => {
        if(needsRewrite(condition)){
          Project(child.output, 
            Filter(
              rewrite(condition, caveats),
              caveats.annotate(child)
            )
          )
        } else { original }
     }
    }

  def needsRewrite(expression: Expression) = 
    expression.collect { 
      case Possible(_,_) => true
    }.exists { x => x }

  def rewrite(
    expression: Expression,
    caveats: CaveatExistsInPlan
  ): Expression = 
    expression.transformUp {
      case Possible(child, context) if child.dataType.equals(BooleanType) => {
        CaveatExistsInExpression.replaceHasCaveat(
          Or(child, 
            ApplyCaveat(
              value = HasCaveat(child),
              message = Literal(context.getOrElse { 
                s"It's possible, but unknown that $expression"
              }),
              // condition = HasCaveat(child)
            )
          )
        )
      }
      case Possible(_, _) => {
        throw new IllegalArgumentException("Possible only supported for Boolean Expression")
      }
    }

}