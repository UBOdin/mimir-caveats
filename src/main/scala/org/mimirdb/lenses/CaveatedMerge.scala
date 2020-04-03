package org.mimirdb.lenses

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType
import org.mimirdb.caveats.ApplyCaveat

object CaveatedMerge
{
  def apply(
    e1: Expression,
    e2: Expression,
    family: Option[String] = None,
    key: Seq[Expression] = Seq()
  ): Expression =
  {
    ApplyCaveat(
      value = e1,
      message = Concat(Seq(
        Literal(s"Expecting $e1 = $e2, but "),
        Cast(e1, StringType),
        Literal(" =!= "),
        Cast(e2, StringType)
      )),
      family = family,
      key = key,
      condition = Not(EqualNullSafe(e1, e2))
    )
  }
}