package org.mimirdb.lenses

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ DataType, StringType }
import org.mimirdb.caveats.ApplyCaveat
import com.typesafe.scalalogging.LazyLogging

object CaveatedCast
  extends LazyLogging
{
  def apply(
    expr: Expression, 
    t: DataType, 
    context: String = null, 
    family: Option[String] = None,
    key: Seq[Expression] = Seq(),
    tzinfo: Option[String] = None
  ): Expression = {
    logger.trace(s"CaveatedCast: $expr -> $t")
    ApplyCaveat(
      value = Cast(expr, t, tzinfo), 
      message = Concat(Seq(
        Literal("Could not cast '"),
        Coalesce(Seq(Cast(expr, StringType), Literal("'NULL'"))),
        Literal(s"' to $t (${Option(context).getOrElse { "in "+expr.toString }})")
      )),
      family = family,
      key = key,
      condition = IsNull(Cast(expr, t))
    )
  }
}