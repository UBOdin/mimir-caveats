package org.mimirdb.caveats.lifting

import org.apache.spark.sql.catalyst.expressions._

/**
 * Lift the upper bound for the target expression into the selected guess
 *
 * When evaluating this expression, the upper bound (the "possible value")
 * will be computed and used as the selected guess instead.  
 *
 * For the time being, Possible is only supported on Boolean expressions in 
 * filter plan operators (WHERE/HAVING).
 */
case class Possible(
  target: Expression,
  context: Option[String]
) extends Expression
  with Unevaluable
{
  def dataType = target.dataType
  def nullable = target.nullable
  def children = Seq(target)
}
