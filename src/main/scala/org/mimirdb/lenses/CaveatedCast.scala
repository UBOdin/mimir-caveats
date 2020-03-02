package org.mimirdb.lenses

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ DataType, StringType }
import org.mimirdb.caveats.ApplyCaveat

object CaveatedCast
{
  def apply(
    expr: Expression, 
    t: DataType, 
    context: String = null, 
    family: Option[String] = None,
    key: Seq[Expression] = Seq()
  ): Expression =
    If( IsNull(Cast(expr, t)), 
        ApplyCaveat(
          value = Literal(null), 
          message = Concat(Seq(
            Literal("Could not cast '"),
            Cast(expr, StringType),
            Literal(s"' to $t (${Option(context).getOrElse { "in "+expr.toString }})")
          )),
          family = family,
          key = key
        ),
        Cast(expr, t)
    )
}