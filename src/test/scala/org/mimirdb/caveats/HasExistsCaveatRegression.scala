package org.mimirdb.caveats

import org.specs2.mutable.Specification
import org.apache.spark.sql.functions._
import org.mimirdb.test._
import org.mimirdb.caveats.implicits._

/**
 * https://github.com/UBOdin/mimir-caveats/issues/9
 */
class HasExistsCaveatRegression
  extends Specification
  with SharedSparkTestInstance
{
  "Handle unix_timestamp" >> {
    spark.read
         .option("header", "true")
         .csv("test_data/timestamp.csv")
         .caveat("Caveat needed to force annotation")
         .createOrReplaceTempView("TEST_HASEXISTSCAVEATREGRESSION_1")


    spark.sql("""
      SELECT id, 
             unix_timestamp(the_time) as the_time 
      FROM TEST_HASEXISTSCAVEATREGRESSION_1
    """).createOrReplaceTempView("TEST_HASEXISTSCAVEATREGRESSION_2")

    var df = 
    spark.sql("""
      SELECT to_timestamp(the_time) as trip_pickup_datetime 
      FROM TEST_HASEXISTSCAVEATREGRESSION_2
    """)

    df = df.trackCaveats
    df = df.stripCaveats

    ok
  }
}