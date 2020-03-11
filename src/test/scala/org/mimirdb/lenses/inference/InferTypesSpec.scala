package org.mimirdb.lenses.inference

import org.specs2.mutable.Specification
import org.mimirdb.test._
import org.apache.spark.sql.types._

class InferTypesSpec 
  extends Specification
  with SharedSparkTestInstance
{
  import spark.implicits._

  lazy val cpuspeed =
    spark.read
         .format("csv")
         .option("header", "true")
         .load("test_data/CPUSpeed.csv")

  lazy val simple =
    spark.createDataFrame(Seq(
      ("23.9", "99", "y"), 
      ("64.2", "92", "fALse")
    )).toDF("float_example", "int_example", "bool_example")

  "InferTypes" >> {

    "Match Numeric Types" >> {
      val tech = InferTypes.inferColumn(
        cpuspeed,
        "Tech. (micron)"
      ).toMap 

      tech must haveKey(DoubleType)
      tech(DoubleType) must be greaterThan(0.9)
      tech must haveKey(FloatType)
      tech(FloatType) must be greaterThan(0.9)
      tech must not haveKey(TimestampType)
      tech must not haveKey(IntegerType)
    }

    "Differentiate Between Ints and Floats" >> {
      {
        val detected = InferTypes.inferColumn(
          simple,
          "int_example"
        )

        detected must not be empty
        detected(0)._1 must be equalTo(ShortType)
      }

      {
        val detected = InferTypes.inferColumn(
          simple,
          "float_example"
        )
        detected must not be empty
        detected(0)._1 must be equalTo(FloatType)
        detected.map { _._1 }.toSet must not contain(IntegerType)
      }
    }

    "Match Boolean Types" >> {
      val detected = 
        InferTypes.inferColumn(simple, "bool_example")
      detected must not be empty
      detected(0)._1 must be equalTo(BooleanType)
    }

    "Match Noisy Types" >> {
      {
        val detected =
          InferTypes.inferColumn(cpuspeed, "Bus speed (MHz)")
        detected must not be empty
        detected(0)._1 must be equalTo(ShortType)
        detected(0)._2 must be lessThan(1.0)
      }
      {
        val detected = 
          InferTypes.inferColumn(cpuspeed, "Cores")
        detected.map { _._1 } must contain(ShortType)
        detected(0)._1 must be equalTo(ShortType)
      }
    }

    "Detect CPUTypes" >> {
      val detected =
        InferTypes(cpuspeed, cutoff = 0.3).map { x => x.name -> x.dataType }.toMap

      detected must havePairs(
        "Processor number" -> StringType,
        "Cores" -> ShortType,
        "Bus speed (MHz)" -> ShortType,
        "CPU speed (GHz)" -> FloatType
      )
    }

  }
}