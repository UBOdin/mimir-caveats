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
    if(Cast.canUpCast(expr.dataType, t)){
      // If canUpCast returns true, then this conversion is guaranteed to be loss-less, and the
      // caveat will never be applied (in which case, we can just return the expression as-is)
      logger.trace(s"CaveatedCast: $expr -> $t (safe)")
      if(expr.dataType.equals(t)){ expr } 
      else { Cast(expr, t, tzinfo) }
    } else {
      logger.trace(s"CaveatedCast: $expr -> $t (caveat needed)")
      return ApplyCaveat(
        value = Cast(expr, t, tzinfo), 
        message = Concat(Seq(
          Literal("Could not cast '"),
          Coalesce(Seq(Cast(expr, StringType, tzinfo), Literal("'NULL'"))),
          Literal(s"' to $t (${Option(context).getOrElse { "in "+expr.toString }})")
        )),
        family = family,
        key = key,
        condition = And(Not(IsNull(expr)), IsNull(Cast(expr, t)))
      )
    }
  }
}