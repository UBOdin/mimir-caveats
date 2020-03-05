package org.mimirdb.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }

object SharedSparkTestInstance
{
  lazy val spark = 
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      .getOrCreate()
  lazy val df = /* R(A int, B int, C int) */
    spark.read
         .format("csv")
         .option("header", "true")
         .load("test_data/r.csv")
}

trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark
  lazy val df = SharedSparkTestInstance.df
}