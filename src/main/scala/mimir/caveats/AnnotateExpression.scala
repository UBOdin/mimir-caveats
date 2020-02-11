package mimir.caveats

import org.apache.spark.sql.catalyst.expressions._

object AnnotateExpression
{
  def apply(expr: NamedExpression) = 
    Alias(annotate(expr), expr.name)()

  def annotate(expr: Expression): Expression = 
  {
    ???
  }
}