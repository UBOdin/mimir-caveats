package org.mimirdb.caveats

import org.specs2.mutable.Specification
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.mimirdb.test._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.Constants._
import java.io.File

class MaterializedViewSpec
  extends Specification
  with SharedSparkTestInstance
{
  val PARQUET_R = "test_data/parquet_r"

  sequential

  "create parquet file" >> {
    if((new File(PARQUET_R)).exists){
      skipped("Parquet file already created")
    }

    var df = spark.read
                  .option("header", "true")
                  .csv("test_data/r.csv")

    df = df.caveatIf("B is null", df("B").isNull)
    df = df.trackCaveats
    df.write.parquet(PARQUET_R)

    ok
  }

  "generate caveats for materialized views" >> {
    var df = spark.read.parquet(PARQUET_R)

    // Materialized views often wrap the view with a projection
    df = df.select(
      df.schema
        .fieldNames
        .filter { !_.equals(ANNOTATION_ATTRIBUTE) }
        .map { df(_) }
      :_*
    )

    // this is the place where runtime problems can occur...
    df = df.trackCaveats

    // df.show()
    val annotations = 
      df.collect().map { _.getAs[Row](3) }

    // and here is the real sanity check.  Exactly one row
    // of R has a null B, so we should have exactly one row caveat
    annotations.filter { _.getAs[Boolean](0) }.size must beEqualTo(1)


    ok

  }
}