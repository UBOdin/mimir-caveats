package org.mimirdb.caveats

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Literal

case class Caveat(
  message: String,
  family: Option[String],
  key: Seq[Literal]
)

object Caveat
{
  def apply(family: Option[String], config:Row): Caveat =
  {
    Caveat(
      message = config.getAs[String](Constants.MESSAGE_ATTRIBUTE),
      family  = family,
      key     = config.getAs[Seq[Any]](Constants.KEY_ATTRIBUTE).map { Literal(_) }
    )
  }
}