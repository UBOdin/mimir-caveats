package org.mimirdb.caveats

import org.apache.spark.sql.Row

case class Caveat(
  message: String,
  family: Option[String],
  key: Seq[Any]
)

object Caveat
{
  def apply(family: Option[String], config:Row): Caveat =
  {
    Caveat(
      message = config.getAs[String](Constants.MESSAGE_ATTRIBUTE),
      family  = family,
      key     = config.getAs[Seq[Any]](Constants.KEY_ATTRIBUTE)
    )
  }
}