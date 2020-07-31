package org.mimirdb.caveats

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.SparkSession

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

  def udfName = "Caveat"

  /**
    * UDF that will be registered with Spark SQL to enable caveating from SQL
    */
  def udf(input: Any): String = // cannot register UDFs that return Any. We never evaluate this function anyways, so just pretend it returns a string
  {
    input.toString()
  }

  def registerUDF(s: SparkSession) = {
    s.udf.register(udfName, udf _)
  }
}
