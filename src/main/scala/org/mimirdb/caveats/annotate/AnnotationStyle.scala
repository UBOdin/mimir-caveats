package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait AnnotationStyle
{
  def apply(
    pedantic: Boolean = true,
    ignoreUnsupported: Boolean = false,
    trace: Boolean = false,
  ): PlanAnnotator
}

trait PlanAnnotator
{
  def apply(plan: LogicalPlan): LogicalPlan
}

