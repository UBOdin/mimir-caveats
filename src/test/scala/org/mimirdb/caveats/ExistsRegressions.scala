package org.mimirdb.caveats

import org.specs2.mutable.Specification
import org.mimirdb.caveats.implicits._
import org.mimirdb.test.SharedSparkTestInstance

class ExistsRegressions
  extends Specification
  with SharedSparkTestInstance
{
  import spark.implicits._

  "Support for Exists Queries" >> 
  {
    val r = spark.read.option("header", "true").csv("test_data/r.csv")
    val u = spark.read.option("header", "true").csv("test_data/u.csv")

    r.createOrReplaceTempView("ExistsRegresssions_ExistsSupport_R")

    u.createOrReplaceTempView("ExistsRegresssions_ExistsSupport_U")

    val q = spark.sql("""
              |SELECT * FROM ExistsRegresssions_ExistsSupport_R r
              |WHERE EXISTS (
              |  SELECT * FROM ExistsRegresssions_ExistsSupport_U u WHERE u.c = r.b
              |)
              """.stripMargin)
    q.trackCaveats.stripCaveats.show()
    ok
  }

}
