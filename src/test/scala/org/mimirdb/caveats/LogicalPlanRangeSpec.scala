package org.mimirdb.caveats

import org.specs2.mutable.Specification


import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, DataFrame, Column, Row }

import org.mimirdb.caveats.Constants._
import org.mimirdb.caveats.annotate._
import org.mimirdb.caveats.implicits._
import org.mimirdb.spark.sparkWorkarounds._
import org.mimirdb.test._

class LogicalPlanRangeSpec
  extends Specification
    with SharedSparkTestInstance
    with DataFrameMatchers
{
  import spark.implicits._

  def trace[T](loggerNames:String*)(op:  => T): T =
  {
    val loggers = loggerNames.map { Logger.getLogger(_) }
    val oldLevels = loggers.map { _.getLevel }
    loggers.foreach { _.setLevel(Level.TRACE) }

    val ret: T = op
    for((logger, oldLevel) <- loggers.zip(oldLevels)){
      logger.setLevel(oldLevel)
    }
    return ret
  }

  def annotWithCombinerToDF(
    input: DataFrame,
    expectedOutput: String,
    trace: Boolean = false
  ) : Boolean =
  {
    val withCaveats = Caveats.annotate(input, CaveatRangeStrategy(), Constants.ANNOTATION_ATTRIBUTE, trace)
    val annotated = withCaveats.planToDF(
      new CaveatRangePlan().combineBestGuess(withCaveats.plan),
    )
    if(trace){
      println("------ FINAL ------")
      println(annotated)
      println(annotated.queryExecution)
      println("PARSED:\n----------\n%s", annotated.queryExecution.logical)
      println("ANALYZED:\n----------\n%s",annotated.queryExecution.analyzed)
      annotated.show(30,100)
    }
    annotated must beBagEqualsTo(expectedOutput)
  }

  def annotBagEqualToDF(
    input: DataFrame,
    expectedOutput: String,
    trace: Boolean = false
  ) : Boolean =
  {
    val annotated = Caveats.annotate(input, CaveatRangeStrategy(), Constants.ANNOTATION_ATTRIBUTE, trace)
    if(trace){
      println("------ FINAL ------")
      println("PARSED:\n----------\n%s", annotated.queryExecution.logical)
      println("ANALYZED:\n----------\n%s",annotated.queryExecution.analyzed)
      annotated.show(30,100)
    }
    annotated must beBagEqualsTo(expectedOutput)
  }

  def annotOrderedEqualToDF(
    input: DataFrame,
    expectedOutput: String,
    trace: Boolean = false
  ) : Boolean =
  {
    val annotated = Caveats.annotate(input, CaveatRangeStrategy(), Constants.ANNOTATION_ATTRIBUTE, trace)
    if(trace){
      println("------ FINAL ------")
      println("PARSED:\n----------\n%s", annotated.queryExecution.logical)
      println("ANALYZED:\n----------\n%s",annotated.queryExecution.analyzed)
      annotated.show(30,100)
    }
    annotated must beOrderedEqualsTo(expectedOutput)
  }

  "DataFrame Range Annotations" >> {

    "Certain inputs" >> {

      "constant tuple" >> {
        annotBagEqualToDF(
          dfr.planToDF(
            Project(
              Seq(Alias(Literal(1),"A")(), Alias(Literal(2),"B")()),
              OneRowRelation()
            ),
          ),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|  B|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|  2|               1|               1|               1|             1|             1|             2|             2|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
// , trace = true
        )
      }

      "projections" >> {
        annotBagEqualToDF(
          dfr.select(),
"""
+----------------+----------------+----------------+
|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|
+----------------+----------------+----------------+
|               1|               1|               1|
|               1|               1|               1|
|               1|               1|               1|
|               1|               1|               1|
|               1|               1|               1|
|               1|               1|               1|
|               1|               1|               1|
+----------------+----------------+----------------+
"""
 // , trace = true
        )

        annotBagEqualToDF(
          dfr.select($"A"),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  1|               1|               1|               1|             1|             1|
|  1|               1|               1|               1|             1|             1|
|  2|               1|               1|               1|             2|             2|
|  1|               1|               1|               1|             1|             1|
|  1|               1|               1|               1|             1|             1|
|  2|               1|               1|               1|             2|             2|
|  4|               1|               1|               1|             4|             4|
+---+----------------+----------------+----------------+--------------+--------------+
"""
  // , trace = true
        )

        annotBagEqualToDF(
          dfr.select((($"A" + $"B") * 2).as("X")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
| 6.0|               1|               1|               1|           6.0|           6.0|
| 8.0|               1|               1|               1|           8.0|           8.0|
|null|               1|               1|               1|          null|          null|
| 6.0|               1|               1|               1|           6.0|           6.0|
|10.0|               1|               1|               1|          10.0|          10.0|
| 8.0|               1|               1|               1|           8.0|           8.0|
|12.0|               1|               1|               1|          12.0|          12.0|
+----+----------------+----------------+----------------+--------------+--------------+
"""
  // , trace = true
        )

        annotBagEqualToDF(
          dfr.select($"A", $"C"),
"""
+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|   C|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_C_LB|__CAVEATS_C_UB|
+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|   3|               1|               1|               1|             1|             1|             3|             3|
|  1|   1|               1|               1|               1|             1|             1|             1|             1|
|  2|   1|               1|               1|               1|             2|             2|             1|             1|
|  1|null|               1|               1|               1|             1|             1|          null|          null|
|  1|   2|               1|               1|               1|             1|             1|             2|             2|
|  2|   1|               1|               1|               1|             2|             2|             1|             1|
|  4|   4|               1|               1|               1|             4|             4|             4|             4|
+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
// , trace = true
        )

        annotBagEqualToDF(
          dfr.select($"A", $"C", when($"C" === 1, $"A").otherwise($"C").as("X")),
"""
+---+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|  B|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|  2|  2|               1|               1|               1|             1|             1|             2|             2|             2|             2|
|  1|  3|  3|               0|               1|               1|             1|             1|             3|             3|             3|             3|
|  2|  1|  2|               0|               1|               1|             2|             2|             1|             1|             2|             2|
|  3|  3|  3|               0|               0|               1|             3|             3|             3|             3|             3|             3|
+---+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
 , trace = true
        )

      }

      "filter" >> {

        annotBagEqualToDF(
          dfr.filter { $"A" =!= 1 and $"B" < $"C"},
"""
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  C|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_C_LB|__CAVEATS_C_UB|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  4|   2|  4|               1|               1|               1|             4|             4|             2|             2|             4|             4|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
//  , trace = true
        )

        annotBagEqualToDF(
          dfr.filter { $"A" =!= 1 },
"""
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  C|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_C_LB|__CAVEATS_C_UB|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  2|null|  1|               1|               1|               1|             2|             2|          null|          null|             1|             1|
|  2|   2|  1|               1|               1|               1|             2|             2|             2|             2|             1|             1|
|  4|   2|  4|               1|               1|               1|             4|             4|             2|             2|             4|             4|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
//   , trace = true
        )

      }

      "union" >> {

        annotBagEqualToDF(
          dfr.select($"A").union(dfr.select($"B")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|   1|               1|               1|               1|             1|             1|
|   1|               1|               1|               1|             1|             1|
|   2|               1|               1|               1|             2|             2|
|   1|               1|               1|               1|             1|             1|
|   1|               1|               1|               1|             1|             1|
|   2|               1|               1|               1|             2|             2|
|   4|               1|               1|               1|             4|             4|
|   2|               1|               1|               1|             2|             2|
|   3|               1|               1|               1|             3|             3|
|null|               1|               1|               1|          null|          null|
|   2|               1|               1|               1|             2|             2|
|   4|               1|               1|               1|             4|             4|
|   2|               1|               1|               1|             2|             2|
|   2|               1|               1|               1|             2|             2|
+----+----------------+----------------+----------------+--------------+--------------+
"""
  , trace = true
        )
      }


      "joins" >> {
        annotBagEqualToDF(
          dfr.select($"A", $"B").join(dfs.filter($"D" === 2), $"A" === $"D"),
"""
+---+----+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  D|   E|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_D_LB|__CAVEATS_D_UB|__CAVEATS_E_LB|__CAVEATS_E_UB|
+---+----+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  2|null|  2|null|               1|               1|               1|             2|             2|          null|          null|             2|             2|          null|          null|
|  2|null|  2|   2|               1|               1|               1|             2|             2|          null|          null|             2|             2|             2|             2|
|  2|   2|  2|null|               1|               1|               1|             2|             2|             2|             2|             2|             2|          null|          null|
|  2|   2|  2|   2|               1|               1|               1|             2|             2|             2|             2|             2|             2|             2|             2|
+---+----+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
         // , trace = true
        )
      }
    }

    // "support aggregates without caveats" >> {
    //   annotate(
    //     dfr.select( sum($"A").as("X") )
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(ones)
    //   }

    //   annotate(
    //     dfr.select( $"A", $"B".cast("int").as("B") )
    //       .groupBy($"A").sum("B")
    //     // ,trace = true
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(false, false, false))
    //   }
    // }

    "TIP inputs" >> {

      "tableaccess" >> {
        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
             // , trace = true
          ),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|  B|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|  2|               1|               1|               1|             1|             1|             2|             2|
|  1|  3|               0|               1|               1|             1|             1|             3|             3|
|  2|  1|               0|               1|               1|             2|             2|             1|             1|
|  3|  3|               0|               0|               1|             3|             3|             3|             3|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
            // , trace = true
        )
      }

      "projections" >> {
        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
             // , trace = true
          ).select($"A", $"B", when($"B" === 1, $"A").otherwise($"B").as("X")),
"""
+---+----+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   C|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_C_LB|__CAVEATS_C_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+----+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|   3|   3|               1|               1|               1|             1|             1|             3|             3|             3|             3|
|  1|   1|   1|               1|               1|               1|             1|             1|             1|             1|             1|             1|
|  2|   1|   2|               1|               1|               1|             2|             2|             1|             1|             2|             2|
|  1|null|null|               1|               1|               1|             1|             1|          null|          null|             1|             1|
|  1|   2|   2|               1|               1|               1|             1|             1|             2|             2|             2|             2|
|  2|   1|   2|               1|               1|               1|             2|             2|             1|             1|             2|             2|
|  4|   4|   4|               1|               1|               1|             4|             4|             4|             4|             4|             4|
+---+----+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
 , trace = true
        )

      }

      "filter" >> {

        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).filter( $"A" === 1 or $"A" === 3 ),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|  B|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|  2|               1|               1|               1|             1|             1|             2|             2|
|  1|  3|               0|               1|               1|             1|             1|             3|             3|
|  3|  3|               0|               0|               1|             3|             3|             3|             3|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
       //     , trace = true
        )

      }

      "union" >> {

        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).select($"A")
            .union(dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).select($"B")),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  1|               1|               1|               1|             1|             1|
|  1|               0|               1|               1|             1|             1|
|  2|               0|               1|               1|             2|             2|
|  3|               0|               0|               1|             3|             3|
|  2|               1|               1|               1|             2|             2|
|  3|               0|               1|               1|             3|             3|
|  1|               0|               1|               1|             1|             1|
|  3|               0|               0|               1|             3|             3|
+---+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).select($"A")
            .union(dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
            ).select($"B"))
            .union(dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
            ).select($"B")),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  1|               1|               1|               1|             1|             1|
|  1|               0|               1|               1|             1|             1|
|  2|               0|               1|               1|             2|             2|
|  3|               0|               0|               1|             3|             3|
|  2|               1|               1|               1|             2|             2|
|  3|               0|               1|               1|             3|             3|
|  1|               0|               1|               1|             1|             1|
|  3|               0|               0|               1|             3|             3|
|  2|               1|               1|               1|             2|             2|
|  3|               0|               1|               1|             3|             3|
|  1|               0|               1|               1|             1|             1|
|  3|               0|               0|               1|             3|             3|
+---+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )
      }

      "joins" >> {

        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
  //            , trace = true
          )
            .select( $"A" ).join(
              dftip.uncertainToAnnotation(
                TupleIndependentProbabilisticDatabase("P"),
                CaveatRangeStrategy()
    //              , trace = true
              )
                .select( $"B"),
              $"A" === $"B"
            ),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|  B|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|  1|               0|               1|               1|             1|             1|             1|             1|
|  1|  1|               0|               1|               1|             1|             1|             1|             1|
|  2|  2|               0|               1|               1|             2|             2|             2|             2|
|  3|  3|               0|               0|               1|             3|             3|             3|             3|
|  3|  3|               0|               0|               1|             3|             3|             3|             3|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
//           , trace = true
        )

      }

      "best guess combiner" >> {

        annotWithCombinerToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).select($"A"),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  1|               1|               2|               2|             1|             1|
|  2|               0|               1|               1|             2|             2|
|  3|               0|               0|               1|             3|             3|
+---+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

        annotWithCombinerToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).select($"A")
            .union(dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
         //    , trace = true
          ).select($"B")),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  1|               1|               3|               3|             1|             1|
|  2|               1|               2|               2|             2|             2|
|  3|               0|               1|               3|             3|             3|
+---+----------------+----------------+----------------+--------------+--------------+
"""
 // , trace = true
        )
      }


    }

    // "support order by/limit without caveats" >> {
    //   annotate(
    //     dfr.sort( $"A" )
    //       .limit(2)
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(false, false))
    //     result.map { _._2("B") } must be equalTo(Seq(false, false))
    //   }

    //   annotate(
    //     dfr.select( $"A", $"B".cast("int").as("B"))
    //       .groupBy($"A").agg( sum($"B").as("B") )
    //       .sort( $"B".desc )
    //       .limit(1)
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(false))
    //     result.map { _._2("A") } must be equalTo(Seq(false))
    //   }
    // }

    // "support projection with caveats" >> {
    //   annotate(
    //     dfr.limit(3)
    //       .select( $"A".caveat("An Issue!").as("A"), $"B" )
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(false, false, false))
    //     result.map { _._2("A") } must be equalTo(Seq(true, true, true))
    //     result.map { _._2("B") } must be equalTo(Seq(false, false, false))
    //   }
    //   annotate(
    //     dfr.limit(3)
    //       .select(
    //         when($"A" === 1, $"A".caveat("A=1"))
    //           .otherwise($"A").as("A"),
    //         $"B"
    //       )
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(false, false, false))
    //     result.map { _._2("A") } must be equalTo(Seq(true, true, false))
    //     result.map { _._2("B") } must be equalTo(Seq(false, false, false))
    //   }
    // }

    // "support selection with caveats" >> {
    //   annotate(
    //     dfr.filter { ($"A" === 1).caveat("Is this right?") }
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(true, true, true, true))
    //     result.map { _._2("A") } must be equalTo(Seq(false, false, false, false))
    //   }

    //   annotate(
    //     dfr.select(
    //       when($"B" === 2, $"B".caveat("Huh?"))
    //         .otherwise($"B").as("B"),
    //       $"A"
    //     ).filter { $"B" =!= 3 }
    //      .limit(3)
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(true, true, false))
    //     result.map { _._2("A") } must be equalTo(Seq(false, false, false))
    //     result.map { _._2("B") } must be equalTo(Seq(true, true, false))
    //   }
    // }

    // "support aggregation with caveats" >> {
    //   annotate(
    //     dfr.select(
    //       $"A", $"B",
    //       when($"C" === 1, $"C".caveat("Sup."))
    //         .otherwise($"C").as("C")
    //     ).groupBy("A")
    //      .agg( sum($"C").as("C") )
    //      .sort($"A")
    //   ) { result =>
    //     result.map { _._1 } must be equalTo(Seq(false, false, false))
    //     result.map { _._2("A") } must be equalTo(Seq(false, false, false))
    //     result.map { _._2("C") } must be equalTo(Seq(true, true, false))
    //   }

    //   annotate(
    //     dfr.select(
    //       $"A", $"B",
    //       when($"C" === 1, $"C".caveat("Dude."))
    //         .otherwise($"C").as("C")
    //     ).filter { $"C" <= 3 }
    //      .groupBy("A")
    //      .agg( sum($"C").as("C") )
    //      .sort($"A")
    //     // ,trace=true
    //   ) { result =>
    //     // 1,2,3
    //     // 1,3,1* !
    //     // 1,2,
    //     // 1,4,2
    //     //   `----> 1, 6*
    //     // 2, ,1* !
    //     // 2,2,1* !
    //     //   `----> 2, 2* !
    //     //X4X2X4XX <- removed by filter


    //     result.map { _._1 } must be equalTo(Seq(false, true))
    //     result.map { _._2("A") } must be equalTo(Seq(false, false))
    //     result.map { _._2("C") } must be equalTo(Seq(true, true))
    //   }

    //   annotate(
    //     dfr.select($"A".cast("int"), $"B".cast("int"), $"C".cast("int"))
    //       .select(
    //         $"A",
    //         when($"B" === 2, $"B".caveat("Dude."))
    //           .otherwise($"B").as("B"),
    //         when($"C" === 1, $"C".caveat("Dude."))
    //           .otherwise($"C").as("C")
    //       )
    //       .filter { $"B" > 1 }
    //       .groupBy("C")
    //       .agg( sum($"A").as("A") )
    //       .sort($"C")
    //     // ,trace = true
    //   ) { result =>
    //     // 1,2*,x  !
    //     //   `----> x, 1* !
    //     // 1,3 ,1*
    //     // 2,  ,1*
    //     // 2,2*,1* !
    //     //   `----> 1, 3* !
    //     // 1,4 ,2
    //     //   `----> 2, 1*
    //     // 1,2*,3  !
    //     //   `----> 3, 1* !
    //     // 4,2*,4  !
    //     //   `----> 4, 4* !
    //     result.map { _._1 } must be equalTo(Seq(true, true, false, true, true))
    //     result.map { _._2("A") } must be equalTo(Seq(true, true, true, true, true))

    //     // skipping the following test due to limitations of Spark's aggregate
    //     // representation: distinguishing group-by fragments of an attribute
    //     // from the rest is painful, so "C" is going to unfortunately get
    //     // attribute-annotated as well.
    //     // result.map { _._2("C") } must be equalTo(Seq(false, false, false, false, false))
    //   }

    //   annotate(
    //     dfr.select($"A".cast("int"), $"B".cast("int"), $"C".cast("int"))
    //       .select(
    //         $"A",
    //         when($"B" === 2, $"B".caveat("Dude."))
    //           .otherwise($"B").as("B"),
    //         when($"C" === 1, $"C".caveat("Dude."))
    //           .otherwise($"C").as("C")
    //       )
    //       .filter { $"B" > 1 }
    //       .groupBy("C")
    //       .agg( sum($"A").as("A") )
    //       .sort($"C"),
    //     pedantic = false
    //     // ,trace = true
    //   ) { result =>
    //     // 1,2*,x  !
    //     //   `----> x, 1* !
    //     // 1,3 ,1*
    //     // 2,  ,1*
    //     // 2,2*,1* !
    //     //   `----> 1, 3* !
    //     // 1,4 ,2
    //     //   `----> 2, 1
    //     // 1,2*,3  !
    //     //   `----> 3, 1* !
    //     // 4,2*,4  !
    //     //   `----> 4, 4* !

    //     // pedantry shouldn't affect the row-annotations
    //     result.map { _._1 } must be equalTo(Seq(true, true, false, true, true))
    //     // but we should get back clean results for the one row that could
    //     // only be affected by a record in another group sneaking in
    //     result.map { _._2("A") } must be equalTo(Seq(true, true, false, true, true))
    //   }
    // }

  }
}
