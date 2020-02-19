package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait AnnotationStyle
{
  def apply(plan: LogicalPlan): LogicalPlan
}

