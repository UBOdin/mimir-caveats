/* -- copyright-header:v1 --
 * Copyright (C) 2017-2020 University at Buffalo,
 *                         New York University,
 *                         Illinois Institute of Technology.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -- copyright-header:end -- */
package org.mimirdb.caveats

import org.specs2.mutable.Specification

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ SparkSession, DataFrame, Column, Row }
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.Constants._
import org.mimirdb.caveats._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.annotate._
import org.mimirdb.spark.sparkWorkarounds._
import org.mimirdb.test._

class LogicalPlanExistsSpec
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
      d.showWithIntermediateResults(30,100)
      d.explain() // ("codegen")
    }
  }

  def annotBagEqualToDF(
    input: DataFrame,
    expectedOutput: String,
    trace: Boolean = false,
    pedantic: Boolean = true
  ) : Boolean =
  {
    val annotated = Caveats.annotate(input, CaveatExists(
                                                trace = trace,
                                                pedantic = pedantic))
    tracePlan(annotated,trace)
    annotated must beBagEqualsTo(expectedOutput)
  }

  def annotate[T](
    input: DataFrame,
    trace: Boolean = false,
    pedantic: Boolean = true
  )( op : Seq[(Boolean, Map[String,Boolean])] => T) =
  {
    val annotated = Caveats.annotate(input, CaveatExists(
                                                trace = trace,
                                                pedantic = pedantic))
    if(trace){
      println("------ FINAL ------")
      println(annotated.queryExecution.analyzed)
    }
    op(
      annotated
       .collect()
       .map { row =>
         val annotation = row.getAs[Row](ANNOTATION_ATTRIBUTE)
         val attributes = annotation.getAs[Row](ATTRIBUTE_FIELD)
         if(trace){ println( "ROW: "+row ) }
         (
           annotation.getAs[Boolean](ROW_FIELD),
           attributes.schema
                     .fields
                     .map { _.name }
                     .map { name => name -> attributes.getAs[Boolean](name) }
                     .toMap
         )
       }
    )
  }

  "support simple operators without caveats" >> {
    annotate(
      dfr.select()
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false, false, false, false, false, false, false))
    }

    annotate(
      dfr.filter { $"A" =!= 1 }
    ) { result =>
        result.map { _._1 } must be equalTo(Seq(false, false, false))
    }
  }

  "support aggregates without caveats" >> {
    annotate(
      dfr.select( sum($"A") )
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false))
    }
    annotate(
      dfr.select( $"A", $"B".cast("int").as("B") )
        .groupBy($"A").sum("B")
      // ,trace = true
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false, false, false))
    }
  }

  "support order by/limit without caveats" >> {
    annotate(
      dfr.sort( $"A" )
        .limit(2)
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false, false))
      result.map { _._2("B") } must be equalTo(Seq(false, false))
    }

    annotate(
      dfr.select( $"A", $"B".cast("int").as("B"))
        .groupBy($"A").agg( sum($"B").as("B") )
        .sort( $"B".desc )
        .limit(1)
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false))
      result.map { _._2("A") } must be equalTo(Seq(false))
    }
  }

  "support projection with caveats" >> {
    annotate(
      dfr.limit(3)
        .select( $"A".caveat("An Issue!").as("A"), $"B" )
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false, false, false))
      result.map { _._2("A") } must be equalTo(Seq(true, true, true))
      result.map { _._2("B") } must be equalTo(Seq(false, false, false))
    }
    annotate(
      dfr.limit(3)
        .select(
          when($"A" === 1, $"A".caveat("A=1"))
            .otherwise($"A").as("A"),
          $"B"
        )
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false, false, false))
      result.map { _._2("A") } must be equalTo(Seq(true, true, false))
      result.map { _._2("B") } must be equalTo(Seq(false, false, false))
    }
  }

  "support selection with caveats" >> {
    annotate(
      dfr.filter { ($"A" === 1).caveat("Is this right?") }
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(true, true, true, true))
      result.map { _._2("A") } must be equalTo(Seq(false, false, false, false))
    }

    annotate(
      dfr.select(
        when($"B" === 2, $"B".caveat("Huh?"))
          .otherwise($"B").as("B"),
        $"A"
      ).filter { $"B" =!= 3 }
       .limit(3)
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(true, true, false))
      result.map { _._2("A") } must be equalTo(Seq(false, false, false))
      result.map { _._2("B") } must be equalTo(Seq(true, true, false))
    }
  }

  "support aggregation with caveats" >> {
    annotate(
      dfr.select(
        $"A", $"B",
        when($"C" === 1, $"C".caveat("Sup."))
          .otherwise($"C").as("C")
      ).groupBy("A")
       .agg( sum($"C").as("C") )
       .sort($"A")
    ) { result =>
      result.map { _._1 } must be equalTo(Seq(false, false, false))
      result.map { _._2("A") } must be equalTo(Seq(false, false, false))
      result.map { _._2("C") } must be equalTo(Seq(true, true, false))
    }

    annotate(
      dfr.select(
        $"A", $"B",
        when($"C" === 1, $"C".caveat("Dude."))
          .otherwise($"C").as("C")
      ).filter { $"C" <= 3 }
       .groupBy("A")
       .agg( sum($"C").as("C") )
       .sort($"A")
      // ,trace=true
    ) { result =>
      // 1,2,3
      // 1,3,1* !
      // 1,2,
      // 1,4,2
      //   `----> 1, 6*
      // 2, ,1* !
      // 2,2,1* !
      //   `----> 2, 2* !
      //X4X2X4XX <- removed by filter


      result.map { _._1 } must be equalTo(Seq(false, true))
      result.map { _._2("A") } must be equalTo(Seq(false, false))
      result.map { _._2("C") } must be equalTo(Seq(true, true))
    }

    annotate(
      dfr.select($"A".cast("int"), $"B".cast("int"), $"C".cast("int"))
        .select(
          $"A",
          when($"B" === 2, $"B".caveat("Dude."))
            .otherwise($"B").as("B"),
          when($"C" === 1, $"C".caveat("Dude."))
            .otherwise($"C").as("C")
        )
        .filter { $"B" > 1 }
        .groupBy("C")
        .agg( sum($"A").as("A") )
        .sort($"C")
      // ,trace = true
    ) { result =>
      // 1,2*,x  !
      //   `----> x, 1* !
      // 1,3 ,1*
      // 2,  ,1*
      // 2,2*,1* !
      //   `----> 1, 3* !
      // 1,4 ,2
      //   `----> 2, 1*
      // 1,2*,3  !
      //   `----> 3, 1* !
      // 4,2*,4  !
      //   `----> 4, 4* !
      result.map { _._1 } must be equalTo(Seq(true, true, false, true, true))
      result.map { _._2("A") } must be equalTo(Seq(true, true, true, true, true))

      // skipping the following test due to limitations of Spark's aggregate
      // representation: distinguishing group-by fragments of an attribute
      // from the rest is painful, so "C" is going to unfortunately get
      // attribute-annotated as well.
      // result.map { _._2("C") } must be equalTo(Seq(false, false, false, false, false))
    }

    annotate(
      dfr.select($"A".cast("int"), $"B".cast("int"), $"C".cast("int"))
        .select(
          $"A",
          when($"B" === 2, $"B".caveat("Dude."))
            .otherwise($"B").as("B"),
          when($"C" === 1, $"C".caveat("Dude."))
            .otherwise($"C").as("C")
        )
        .filter { $"B" > 1 }
        .groupBy("C")
        .agg( sum($"A").as("A") )
        .sort($"C"),
      pedantic = false
      // ,trace = true
    ) { result =>
      // 1,2*,x  !
      //   `----> x, 1* !
      // 1,3 ,1*
      // 2,  ,1*
      // 2,2*,1* !
      //   `----> 1, 3* !
      // 1,4 ,2
      //   `----> 2, 1
      // 1,2*,3  !
      //   `----> 3, 1* !
      // 4,2*,4  !
      //   `----> 4, 4* !

      // pedantry shouldn't affect the row-annotations
      result.map { _._1 } must be equalTo(Seq(true, true, false, true, true))
      // but we should get back clean results for the one row that could
      // only be affected by a record in another group sneaking in
      result.map { _._2("A") } must be equalTo(Seq(true, true, false, true, true))
    }
  }

  "Support Annotation Re-Use" >>  {
    val base = Caveats.annotate(
      dfr.select($"A".caveat("HI!").as("A"))
    )

    annotate(
      base
    ) { result =>
      result.size must beGreaterThan(1)
      val (row, fields) = result(0)
      fields("A") must be equalTo(true)
      row must beFalse
    }

    annotate(
      base.filter(base("A") === 4)
    ) { result =>
      result.size must be equalTo(1)
      val (row, fields) = result(0)
      fields("A") must be equalTo(true)
      row must beTrue
    }

  }

  "support order by/limit with caveats" >> {
    annotate(
      dfr.select(  $"A".caveatIf("HI!", $"A" === 2 ).as("A"), $"B" )
        .sort( $"A" )
        .limit(5)
    ) { result =>
      // These are just the baseline annotations as a sanity check.  If these are wrong,
      // then the limit on r.csv is returning a different subset of records than expected
      result.map { _._2("A") } must be equalTo(Seq(false, false, false, false, true))
      result.map { _._2("B") } must be equalTo(Seq(false, false, false, false, false))

      // We expect that the row annotations should contain at least one true.  Technically
      // all of the one records (the first four rows) can be safely marked false, but the
      // annotation scheme isn't quite that clever (yet)
      result.map { _._1 } must contain(true)
      // result.map { _._1 } must be equalTo(Seq(false, false, false, false, true))
    }
  }

  "support outer joins" >> {
    dfr.createTempView("TEST_R_OUTER_JOINS")
    annotate(
      spark.sql("""
        SELECT DISTINCT a.B FROM TEST_R_OUTER_JOINS a
          LEFT JOIN TEST_R_OUTER_JOINS b ON b.A = a.A
        WHERE b.B IS NULL
      """)
    ) { result => 
      result.map { _._1 } must not contain(true)
    }
  }

  "support caveat function in SQL" >> {
    // register tables and UDF
    Caveat.registerUDF(spark)
    registerSQLtables()

    annotBagEqualToDF(spark.sql("SELECT Caveat(A) AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[true]]|
|  1|[false,[true]]|
|  2|[false,[true]]|
|  1|[false,[true]]|
|  1|[false,[true]]|
|  2|[false,[true]]|
|  4|[false,[true]]|
+---+----------------+
"""
// , trace = true
    )

    annotBagEqualToDF(spark.sql("SELECT Caveat(A,'my_message') AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[true]]|
|  1|[false,[true]]|
|  2|[false,[true]]|
|  1|[false,[true]]|
|  1|[false,[true]]|
|  2|[false,[true]]|
|  4|[false,[true]]|
+---+----------------+
"""
// , trace = true
    )

    annotBagEqualToDF(spark.sql("SELECT Caveat(A,'my_message', B) AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[true]]|
|  1|[false,[true]]|
|  2|[false,[true]]|
|  1|[false,[true]]|
|  1|[false,[true]]|
|  2|[false,[true]]|
|  4|[false,[true]]|
+---+----------------+
"""
// , trace = true
    )

    annotBagEqualToDF(spark.sql("SELECT CaveatIf(A,A=1) AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[true]]  |
|  1|[false,[true]]  |
|  2|[false,[false]] |
|  1|[false,[true]]  |
|  1|[false,[true]]  |
|  2|[false,[false]] |
|  4|[false,[false]] |
+---+----------------+
"""
// , trace = true
    )

    annotBagEqualToDF(spark.sql("SELECT CaveatIfNull(CASE WHEN A = 1 THEN A ELSE NULL END) AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[false]]  |
|  1|[false,[false]]  |
|  null|[false,[true]] |
|  1|[false,[false]]  |
|  1|[false,[false]]  |
|  null|[false,[true]] |
|  null|[false,[true]] |
+---+----------------+
"""
// , trace = true
    )

    annotBagEqualToDF(spark.sql("SELECT CaveatReplaceIf(A,10,A<>1) AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[false]]  |
|  1|[false,[false]]  |
|  10|[false,[true]] |
|  1|[false,[false]]  |
|  1|[false,[false]]  |
|  10|[false,[true]] |
|  10|[false,[true]] |
+---+----------------+
"""
// , trace = true
    )

    annotBagEqualToDF(spark.sql("SELECT A FROM (SELECT CaveatReplaceIf(A,10,A<>1) AS A FROM r) sub WHERE HasCaveat(A)"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  10|[true,[true]] |
|  10|[true,[true]] |
|  10|[true,[true]] |
+---+----------------+
"""
// , trace = true
    )

    spark.udf.register("myfunkyfunc", (x:String) => x)

    annotBagEqualToDF(spark.sql("SELECT myfunkyfunc(A) AS A FROM r"),
"""
+---+----------------+
|  A|       __CAVEATS|
+---+----------------+
|  1|[false,[false]]|
|  1|[false,[false]]|
|  2|[false,[false]]|
|  1|[false,[false]]|
|  1|[false,[false]]|
|  2|[false,[false]]|
|  4|[false,[false]]|
+---+----------------+
"""
// , trace = true
    )
  }
}

