package org.mimirdb.caveats


import org.specs2.mutable.Specification


import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
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


  def tracePlan(d: DataFrame, trace: Boolean = false) = {
    if(trace)
    {
      println("================================================================================\n                                      FINAL\n================================================================================")
      println("\n============================== QUERY EXECUTION (PLANS) ==============================\n\n")
      d.queryExecution.analyzed
      println(d.queryExecution)
      println("\n============================== SCHEMA ==============================\n")
      println(s"${d.schema}")
      println("\n============================== RESULT ==============================\n")
      //d.show(30,100)
      d.showWithIntermediateResults(30,100)
      d.explain() // ("codegen")
    }
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
    tracePlan(annotated,trace)
    annotated must beBagEqualsTo(expectedOutput)
  }

  def annotBagEqualToDF(
    input: DataFrame,
    expectedOutput: String,
    trace: Boolean = false
  ) : Boolean =
  {
    val annotated = Caveats.annotate(input, CaveatRangeStrategy(), Constants.ANNOTATION_ATTRIBUTE, trace)
    tracePlan(annotated,trace)
    annotated must beBagEqualsTo(expectedOutput)
  }

  def annotOrderedEqualToDF(
    input: DataFrame,
    expectedOutput: String,
    trace: Boolean = false
  ) : Boolean =
  {
    val annotated = Caveats.annotate(input, CaveatRangeStrategy(), Constants.ANNOTATION_ATTRIBUTE, trace)
    tracePlan(annotated,trace)
    annotated must beOrderedEqualsTo(expectedOutput)
  }

  "DataFrame Range Annotations" >> {

    "Certain inputs" >> {

      "certain inputs.constant tuple" >> {
        skipped("Spark 3.2 broke ranges... will come back to fix this")

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

      "certain inputs.projections" >> {
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
// , trace = true
        )

      }

      "certain inputs.filter" >> {

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

      "certain inputs.union" >> {
        skipped("UNION is currently broken.  See CaveatRangePlan.scala")

//         annotBagEqualToDF(
//           dfr.select($"A").union(dfr.select($"B")),
// """
// +----+----------------+----------------+----------------+--------------+--------------+
// |   A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
// +----+----------------+----------------+----------------+--------------+--------------+
// |   1|               1|               1|               1|             1|             1|
// |   1|               1|               1|               1|             1|             1|
// |   2|               1|               1|               1|             2|             2|
// |   1|               1|               1|               1|             1|             1|
// |   1|               1|               1|               1|             1|             1|
// |   2|               1|               1|               1|             2|             2|
// |   4|               1|               1|               1|             4|             4|
// |   2|               1|               1|               1|             2|             2|
// |   3|               1|               1|               1|             3|             3|
// |null|               1|               1|               1|          null|          null|
// |   2|               1|               1|               1|             2|             2|
// |   4|               1|               1|               1|             4|             4|
// |   2|               1|               1|               1|             2|             2|
// |   2|               1|               1|               1|             2|             2|
// +----+----------------+----------------+----------------+--------------+--------------+
// """
// //  , trace = true
//         )
      }


      "certain inputs.joins" >> {
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

        annotBagEqualToDF(
          dfr.select($"A", $"B").join(dfs.filter($"D" === 2), $"A" === $"D").join(dft.filter($"F" === 2), $"D" === $"F"),
"""
+---+----+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  D|   E|  F|  G|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_E_LB|__CAVEATS_E_UB|__CAVEATS_F_LB|__CAVEATS_F_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_G_LB|__CAVEATS_G_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_D_LB|__CAVEATS_D_UB|
+---+----+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  2|null|  2|null|  2|  1|               1|               1|               1|          null|          null|             2|             2|             2|             2|             1|             1|          null|          null|             2|             2|
|  2|null|  2|null|  2|  1|               1|               1|               1|          null|          null|             2|             2|             2|             2|             1|             1|          null|          null|             2|             2|
|  2|null|  2|   2|  2|  1|               1|               1|               1|             2|             2|             2|             2|             2|             2|             1|             1|          null|          null|             2|             2|
|  2|null|  2|   2|  2|  1|               1|               1|               1|             2|             2|             2|             2|             2|             2|             1|             1|          null|          null|             2|             2|
|  2|   2|  2|null|  2|  1|               1|               1|               1|          null|          null|             2|             2|             2|             2|             1|             1|             2|             2|             2|             2|
|  2|   2|  2|null|  2|  1|               1|               1|               1|          null|          null|             2|             2|             2|             2|             1|             1|             2|             2|             2|             2|
|  2|   2|  2|   2|  2|  1|               1|               1|               1|             2|             2|             2|             2|             2|             2|             1|             1|             2|             2|             2|             2|
|  2|   2|  2|   2|  2|  1|               1|               1|               1|             2|             2|             2|             2|             2|             2|             1|             1|             2|             2|             2|             2|
+---+----+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
         // , trace = true
        )
      }

      "certain inputs.aggregation - no group-by - aggregtion functions only" >> {

        annotBagEqualToDF(
          dfr.agg(sum($"A").as("X")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|12.0|               1|               1|               1|          12.0|          12.0|
+----+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

        annotBagEqualToDF(
          dfr.agg(count(lit(1)).as("X")),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  7|               1|               1|               1|             7|             7|
+---+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )


        annotBagEqualToDF(
          dfr.agg(min($"A").as("X")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|1   |               1|               1|               1|          1   |          1   |
+----+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

        annotBagEqualToDF(
          dfr.agg(max($"A").as("X")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|4   |               1 |               1|               1|          4  |          4   |
+----+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

      }

      "certain inputs.aggregation - no group-by - avg" >> {
        skipped("avg agg rewrite not working yet!")
        annotBagEqualToDF(
          dfr.agg(avg($"A").as("X")).select($"X"),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|1.71428571429
|               1|               1|               1|          1.71428571429|          1.71428571429|
+----+----------------+----------------+----------------+--------------+--------------+
"""
  , trace = true
        )
      }

      "certain inputs.aggregation - no group-by - with expressions" >> {

        annotBagEqualToDF(
          dfr.agg((sum($"A".cast("double") + 3.0) * 2.0).as("X")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|66.0|               1|               1|               1|          66.0|          66.0|
+----+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

        annotBagEqualToDF(
          dfr.agg((sum($"A".cast("double")) + sum($"B".cast("double")) * 2.0).as("X")),
"""
+----+----------------+----------------+----------------+--------------+--------------+
|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+----+----------------+----------------+----------------+--------------+--------------+
|42.0|               1|               1|               1|          42.0|          42.0|
+----+----------------+----------------+----------------+--------------+--------------+
"""
//  , trace = true
        )

      }


      "certain inputs.aggregation - with group-by - only aggregates" >> {
        skipped("Spark 3.2 broke ranges... will come back to fix this")

        annotBagEqualToDF(
          dfr.filter(!(isnull($"B"))).groupBy($"A").agg(sum($"B".cast("double")).as("X")),
"""
+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|   X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|11.0|               1|               1|               4|             1|             1|          11.0|          11.0|
|  4| 2.0|               1|               1|               1|             4|             4|           2.0|           2.0|
|  2| 2.0|               1|               1|               1|             2|             2|           2.0|           2.0|
+---+----+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )

        annotBagEqualToDF(
          dfr.filter(!(isnull($"B"))).groupBy($"A").agg(min($"B".cast("double")).as("X")),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|2.0|               1|               1|               4|             1|             1|           2.0|           2.0|
|  4|2.0|               1|               1|               1|             4|             4|           2.0|           2.0|
|  2|2.0|               1|               1|               1|             2|             2|           2.0|           2.0|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )


                annotBagEqualToDF(
          dfr.filter(!(isnull($"B"))).groupBy($"A").agg(max($"B".cast("double")).as("X")),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  A|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  1|4.0|               1|               1|               4|             1|             1|           4.0|           4.0|
|  4|2.0|               1|               1|               1|             4|             4|           2.0|           2.0|
|  2|2.0|               1|               1|               1|             2|             2|           2.0|           2.0|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )


      }

    }

    "TIP inputs" >> {

      "TIP inputs.tableaccess" >> {
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

      "TIP inputs.projections" >> {
        annotBagEqualToDF(
          dftip.uncertainToAnnotation(
            TupleIndependentProbabilisticDatabase("P"),
            CaveatRangeStrategy()
             // , trace = true
          ).select($"A", $"B", when($"B" === 1, $"A").otherwise($"B").as("X")),
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
// , trace = true
        )

      }

      "TIP inputs.filter" >> {

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

      "TIP inputs.union" >> {
        skipped("UNION is currently broken.  See CaveatRangePlan.scala")
// 
//         val A = dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//           ).select($"A")

//         val B = dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//           ).select($"B" as "A")

//         (A union B).showCaveats()

//         annotBagEqualToDF(
//           (A union B),
// """
// +---+----------------+----------------+----------------+--------------+--------------+
// |  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
// +---+----------------+----------------+----------------+--------------+--------------+
// |  1|               1|               1|               1|             1|             1|
// |  1|               0|               1|               1|             1|             1|
// |  2|               0|               1|               1|             2|             2|
// |  3|               0|               0|               1|             3|             3|
// |  2|               1|               1|               1|             2|             2|
// |  3|               0|               1|               1|             3|             3|
// |  1|               0|               1|               1|             1|             1|
// |  3|               0|               0|               1|             3|             3|
// +---+----------------+----------------+----------------+--------------+--------------+
// """
//  // , trace = true
//         )

//         annotBagEqualToDF(
//           dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//           ).select($"A")
//             .union(dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//             ).select($"B"))
//             .union(dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//             ).select($"B")),
// """
// +---+----------------+----------------+----------------+--------------+--------------+
// |  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
// +---+----------------+----------------+----------------+--------------+--------------+
// |  1|               1|               1|               1|             1|             1|
// |  1|               0|               1|               1|             1|             1|
// |  2|               0|               1|               1|             2|             2|
// |  3|               0|               0|               1|             3|             3|
// |  2|               1|               1|               1|             2|             2|
// |  3|               0|               1|               1|             3|             3|
// |  1|               0|               1|               1|             1|             1|
// |  3|               0|               0|               1|             3|             3|
// |  2|               1|               1|               1|             2|             2|
// |  3|               0|               1|               1|             3|             3|
// |  1|               0|               1|               1|             1|             1|
// |  3|               0|               0|               1|             3|             3|
// +---+----------------+----------------+----------------+--------------+--------------+
// """
// //  , trace = true
//         )
      }

      "TIP inputs.joins" >> {

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
            ).join(
              dftip.uncertainToAnnotation(
                TupleIndependentProbabilisticDatabase("P"),
                CaveatRangeStrategy()
    //              , trace = true
              )
                .select($"B".as("C")),
              $"B" === $"C"
            ),
"""
+---+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|  B|  C|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_C_LB|__CAVEATS_C_UB|
+---+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|  1|  1|               0|               1|               1|             1|             1|             1|             1|             1|             1|
|  1|  1|  1|               0|               1|               1|             1|             1|             1|             1|             1|             1|
|  2|  2|  2|               0|               1|               1|             2|             2|             2|             2|             2|             2|
|  3|  3|  3|               0|               0|               1|             3|             3|             3|             3|             3|             3|
|  3|  3|  3|               0|               0|               1|             3|             3|             3|             3|             3|             3|
|  3|  3|  3|               0|               0|               1|             3|             3|             3|             3|             3|             3|
|  3|  3|  3|               0|               0|               1|             3|             3|             3|             3|             3|             3|
+---+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
//           , trace = true
        )

      }

      "TIP inputs.best guess combiner" >> {
        skipped("UNION is currently broken.  See CaveatRangePlan.scala")

//         annotWithCombinerToDF(
//           dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//           ).select($"A"),
// """
// +---+----------------+----------------+----------------+--------------+--------------+
// |  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
// +---+----------------+----------------+----------------+--------------+--------------+
// |  1|               1|               2|               2|             1|             1|
// |  2|               0|               1|               1|             2|             2|
// |  3|               0|               0|               1|             3|             3|
// +---+----------------+----------------+----------------+--------------+--------------+
// """
// //  , trace = true
//         )

//         annotWithCombinerToDF(
//           dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//           ).select($"A")
//             .union(dftip.uncertainToAnnotation(
//             TupleIndependentProbabilisticDatabase("P"),
//             CaveatRangeStrategy()
//          //    , trace = true
//           ).select($"B")),
// """
// +---+----------------+----------------+----------------+--------------+--------------+
// |  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
// +---+----------------+----------------+----------------+--------------+--------------+
// |  1|               1|               3|               3|             1|             1|
// |  2|               1|               2|               2|             2|             2|
// |  3|               0|               1|               3|             3|             3|
// +---+----------------+----------------+----------------+--------------+--------------+
// """
//  // , trace = true
//         )
      }


    }

    "Caveated inputs" >> {

      "Caveated inputs.projections" >> {
        annotBagEqualToDF(
          dfr.select($"A", $"B", $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(1), lit(10), $"A" < 4).as("X")),
"""
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|   2|  1|               1|               1|               1|             1|             1|             2|             2|             1|            10|
|  1|   3|  1|               1|               1|               1|             1|             1|             3|             3|             1|            10|
|  2|null|  2|               1|               1|               1|             2|             2|          null|          null|             1|            10|
|  1|   2|  1|               1|               1|               1|             1|             1|             2|             2|             1|            10|
|  1|   4|  1|               1|               1|               1|             1|             1|             4|             4|             1|            10|
|  2|   2|  2|               1|               1|               1|             2|             2|             2|             2|             1|            10|
|  4|   2|  4|               1|               1|               1|             4|             4|             2|             2|             4|             4|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )

        annotBagEqualToDF(
          dfr.select($"A", $"B",
            (
              ($"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(1), lit(10), $"A" < 4)) +
              ($"B".cast("int").as("A").replaceAndRangeCaveat(lit("oh noooooo!"), lit(5), lit(1), lit(10), isnull($"B")))
            ).as("X")),
"""
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|   2|  3|               1|               1|               1|             1|             1|             2|             2|             3|            12|
|  1|   3|  4|               1|               1|               1|             1|             1|             3|             3|             4|            13|
|  2|null|  7|               1|               1|               1|             2|             2|          null|          null|             2|            20|
|  1|   2|  3|               1|               1|               1|             1|             1|             2|             2|             3|            12|
|  1|   4|  5|               1|               1|               1|             1|             1|             4|             4|             5|            14|
|  2|   2|  4|               1|               1|               1|             2|             2|             2|             2|             3|            12|
|  4|   2|  6|               1|               1|               1|             4|             4|             2|             2|             6|             6|
+---+----+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )

        annotBagEqualToDF(
          dfr.select(
            $"A",
            $"B",
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(0), lit(10), $"A" < 2).as("X"),
            $"B".cast("int").as("A").replaceAndRangeCaveat(lit("oh noooooo!"), lit(2), lit(1), lit(10), isnull($"B")).as("Y")
          ),
"""
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  X|  Y|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|__CAVEATS_Y_LB|__CAVEATS_Y_UB|
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|   2|  1|  2|               1|               1|               1|             1|             1|             2|             2|             0|            10|             2|             2|
|  1|   3|  1|  3|               1|               1|               1|             1|             1|             3|             3|             0|            10|             3|             3|
|  2|null|  2|  2|               1|               1|               1|             2|             2|          null|          null|             2|             2|             1|            10|
|  1|   2|  1|  2|               1|               1|               1|             1|             1|             2|             2|             0|            10|             2|             2|
|  1|   4|  1|  4|               1|               1|               1|             1|             1|             4|             4|             0|            10|             4|             4|
|  2|   2|  2|  2|               1|               1|               1|             2|             2|             2|             2|             2|             2|             2|             2|
|  4|   2|  4|  2|               1|               1|               1|             4|             4|             2|             2|             4|             4|             2|             2|
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
// , trace = true
        )


      }

      "Caveated inputs.filter" >> {
        annotBagEqualToDF(
          dfr.select($"A",
            $"B",
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(1), lit(4), $"A" < 4).as("X"),
            $"B".cast("int").as("A").replaceAndRangeCaveat(lit("oh noooooo!"), lit(2), lit(1), lit(10), isnull($"B")).as("Y")
          ).filter($"X" === $"Y"),
"""
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  X|  Y|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|__CAVEATS_Y_LB|__CAVEATS_Y_UB|
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|   2|  1|  2|               0|               0|               1|             1|             1|             2|             2|             1|             4|             2|             2|
|  1|   3|  1|  3|               0|               0|               1|             1|             1|             3|             3|             1|             4|             3|             3|
|  2|null|  2|  2|               0|               1|               1|             2|             2|          null|          null|             1|             4|             1|            10|
|  1|   2|  1|  2|               0|               0|               1|             1|             1|             2|             2|             1|             4|             2|             2|
|  1|   4|  1|  4|               0|               0|               1|             1|             1|             4|             4|             1|             4|             4|             4|
|  2|   2|  2|  2|               0|               1|               1|             2|             2|             2|             2|             1|             4|             2|             2|
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )

        annotBagEqualToDF(
          dfr.select($"A",
            $"B",
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(1), lit(4), $"A" < 2).as("X"),
            $"B".cast("int").as("A").replaceAndRangeCaveat(lit("oh noooooo!"), lit(2), lit(1), lit(10), isnull($"B")).as("Y")
          ).filter($"X" === $"Y"),
"""
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  A|   B|  X|  Y|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|__CAVEATS_B_LB|__CAVEATS_B_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|__CAVEATS_Y_LB|__CAVEATS_Y_UB|
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|  1|   2|  1|  2|               0|               0|               1|             1|             1|             2|             2|             1|             4|             2|             2|
|  1|   3|  1|  3|               0|               0|               1|             1|             1|             3|             3|             1|             4|             3|             3|
|  2|null|  2|  2|               0|               1|               1|             2|             2|          null|          null|             2|             2|             1|            10|
|  1|   2|  1|  2|               0|               0|               1|             1|             1|             2|             2|             1|             4|             2|             2|
|  1|   4|  1|  4|               0|               0|               1|             1|             1|             4|             4|             1|             4|             4|             4|
|  2|   2|  2|  2|               1|               1|               1|             2|             2|             2|             2|             2|             2|             2|             2|
+---+----+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
"""
  // , trace = true
        )
      }

      "Caveated inputs.aggregation without group-by" >> {
        skipped("Spark 3.2 broke ranges... will come back to fix this")

        annotBagEqualToDF(
          dfr.select(
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(0), lit(10), $"A" < 2).as("X")
          ).agg(sum($"X").as("S")),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  S|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_S_LB|__CAVEATS_S_UB|
+---+----------------+----------------+----------------+--------------+--------------+
| 12|               1|               1|               1|            8 |            48|
+---+----------------+----------------+----------------+--------------+--------------+
"""
// , trace = true
        )

        annotBagEqualToDF(
          dfr.select(
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(0), lit(10), $"A" < 2).as("X"),
            $"B".cast("int").as("A").replaceAndRangeCaveat(lit("oh noooooo!"), lit(2), lit(1), lit(10), isnull($"B")).as("Y")
          ).agg(sum($"X").as("S")),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  S|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_S_LB|__CAVEATS_S_UB|
+---+----------------+----------------+----------------+--------------+--------------+
| 12|               1|               1|               1|            8 |            48|
+---+----------------+----------------+----------------+--------------+--------------+
"""
// , trace = true
        )

      }

      "Caveated inputs.aggregation with group-by" >> {
        skipped("Spark 3.2 broke ranges... will come back to fix this")

        annotBagEqualToDF(
          dfr.select(
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(0), lit(10), $"A" < 2).as("X"),
            $"B".cast("int").as("A").replaceAndRangeCaveat(lit("oh noooooo!"), lit(2), lit(1), lit(10), isnull($"B")).as("Y")
          ).groupBy($"Y").agg(sum($"X").as("S")),
"""
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  Y|  S|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_Y_LB|__CAVEATS_Y_UB|__CAVEATS_S_LB|__CAVEATS_S_UB|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
|  3|  1|               1|               1|               1|             3|             3|             0|            12|
|  4|  1|               1|               1|               1|             4|             4|             0|            12|
|  2| 10|               0|               1|               5|             1|            10|             0|            48|
+---+---+----------------+----------------+----------------+--------------+--------------+--------------+--------------+
"""
// , trace = true
        )

      }

      "Caveated inputs.deduplication" >> {
        skipped("Spark 3.2 broke ranges... will come back to fix this")

        annotBagEqualToDF(
          dfr.select(
            $"A".cast("int").as("A").rangeCaveatIf(lit("oh noooooo!"), lit(0), lit(10), $"A" < 2).as("X")
          ).dropDuplicates(),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  X|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_X_LB|__CAVEATS_X_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  2|               1|               1|               2|             2|             2|
|  4|               1|               1|               1|             4|             4|
|  1|               0|               1|               4|             0|            10|
+---+----------------+----------------+----------------+--------------+--------------+
"""
// , trace = true
        )

      }

      "Caveated inputs.caveat udf" >> {
        skipped("Spark 3.2 broke ranges... will come back to fix this")
        ApplyCaveatRange.registerUDF(spark)
        registerSQLtables()

        annotBagEqualToDF(
          spark.sql("SELECT RangeCaveat(A,A-3,A+3) AS A FROM r;"),
"""
+---+----------------+----------------+----------------+--------------+--------------+
|  A|__CAVEATS_ROW_LB|__CAVEATS_ROW_BG|__CAVEATS_ROW_UB|__CAVEATS_A_LB|__CAVEATS_A_UB|
+---+----------------+----------------+----------------+--------------+--------------+
|  1|               1|               1|               1|          -2.0|           4.0|
|  1|               1|               1|               1|          -2.0|           4.0|
|  2|               1|               1|               1|          -1.0|           5.0|
|  1|               1|               1|               1|          -2.0|           4.0|
|  1|               1|               1|               1|          -2.0|           4.0|
|  2|               1|               1|               1|          -1.0|           5.0|
|  4|               1|               1|               1|           1.0|           7.0|
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
