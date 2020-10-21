package org.mimirdb.caveats

import org.specs2.mutable.Specification
import org.apache.spark.sql.functions._
import org.mimirdb.test._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.lifting.ResolveLifts

class LiftSpec
  extends Specification
  with SharedSparkTestInstance
{
  lazy val input = 
    spark .read
          .option("header", "true")
          .csv("test_data/r.csv")
          .select(
            Seq("A", "B", "C").map { c =>
              col(c).caveatIf("NULL", col(c) isNull) as c
            }:_*
          )

  "lift Possible" >> {
    // input.showCaveats()
    val df = input.liftedFilter(
      (input("B") === 2).possible 
      and
      (input("C") === 1)
    )
    // df.showCaveats()

    // There are two matching rows... one deterministic 
    // and one possible only. 
    df.count() must beEqualTo(2l)
  }

  "lift Possible in SQL" >> {
    SharedSparkTestInstance.registerUDFs()
    input.createOrReplaceTempView("LIFT_POSSIBLE_TEST")
    val df = ResolveLifts(spark.sql("""
      SELECT * FROM LIFT_POSSIBLE_TEST
      WHERE POSSIBLE(B = 2) AND C = 1
    """))
    df.showCaveats()
    df.count() must beEqualTo(2l)
  }
}