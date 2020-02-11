package mimir.caveats

import org.specs2.mutable.Specification
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

class ExpressionSpec extends Specification 
{

  lazy val spark = 
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      .getOrCreate()
  lazy val testData = 
    spark.read
         .format("csv")
         .option("header", "true")
         .load("test_data/r.csv")

  "Spark" >> {

    "count the lines" >> {
      import spark.implicits._

      val temp = testData.select($"A", $"B"+1)
      Caveats.annotate(temp) must beAnInstanceOf[Dataset[_]]

    }


  }

}