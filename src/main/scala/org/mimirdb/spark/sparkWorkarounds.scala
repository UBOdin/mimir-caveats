package org.mimirdb.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class DataFrameImplicits(df:DataFrame)
{
  /**
   * DataFrame's .withPlan is private :[
   * 
   * Use the power of implicits to create our own version
   */
  def extend( plan: LogicalPlan ) = 
  {
    new DataFrame(df.sparkSession, plan, RowEncoder(plan.schema))
  }

  def plan = df.queryExecution.logical
}

object sparkWorkarounds
{
  implicit def DataFrameToMimirDataFrame(df:DataFrame) = 
    new DataFrameImplicits(df)
}