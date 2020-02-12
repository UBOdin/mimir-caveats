package org.mimirdb.caveats

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


class AnnotationException(message: String, context: Option[Either[Expression,LogicalPlan]] = None)
  extends Exception(message)
{
  def this(message: String, e: Expression) = this(message, Some(Left(e)))
  def this(message: String, lp: LogicalPlan) = this(message, Some(Right(lp)))
}