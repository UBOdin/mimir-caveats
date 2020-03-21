package org.mimirdb.caveats

import org.specs2.mutable.Specification

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ SparkSession, DataFrame, Column, Row }
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.Constants._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.annotate._

import org.mimirdb.test._

class LogicalPlanRangeSpec
  extends Specification
  with SharedSparkTestInstance
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

  def annotate[T](
    input: DataFrame,
    trace: Boolean = false,
  )( op : Seq[((Int, Int, Int), Map[String,(String, String)])] => T) =
  {
    val annotated = Caveats.annotate(input, CaveatRangeStrategy())
    if(trace){
      println("------ FINAL ------")
      println("PARSED:\n----------\n%s", annotated.queryExecution.logical)
      println("ANALYZED:\n----------\n%s",annotated.queryExecution.analyzed)
      annotated.show(10,100)
    }
    op(
      annotated
       .collect()
       .map { row =>
         val annotation = row.getAs[Row](ANNOTATION_ATTRIBUTE)
         val rowann = annotation.getAs[Row](ROW_FIELD)
         val attributes = annotation.getAs[Row](ATTRIBUTE_FIELD)
         if(trace){ println( "ROW: "+row ) }
         (
           (
             rowann.getAs[Int](LOWER_BOUND_FIELD),
             rowann.getAs[Int](BEST_GUESS_FIELD),
             rowann.getAs[Int](UPPER_BOUND_FIELD),
           ),
           attributes.schema
                     .fields
                     .map { _.name }
                     .map { name =>
                       val r = attributes.getAs[Row](name)
                       name ->
                       (
                         r.getAs[String](LOWER_BOUND_FIELD),
                         r.getAs[String](UPPER_BOUND_FIELD)
                       )
                     }
                     .toMap
         )
       }
    )
  }

  def noAnnotate[T](
    input: DataFrame,
    trace: Boolean = false,
  )( op : Seq[((Int, Int, Int), Map[String,(String, String)])] => T) =
  {
    val annotated = input
    if(trace){
      println("------ FINAL ------")
      println("PARSED:\n----------\n%s", annotated.queryExecution.logical)
      println("ANALYZED:\n----------\n%s",annotated.queryExecution.analyzed)
      println(annotated)
    }
    op(
      annotated
       .collect()
       .map { row =>
         val annotation = row.getAs[Row](ANNOTATION_ATTRIBUTE)
         val rowann = annotation.getAs[Row](ROW_FIELD)
         val attributes = annotation.getAs[Row](ATTRIBUTE_FIELD)
         if(trace){ println( "ROW: "+row ) }
         (
           (
             rowann.getAs[Int](LOWER_BOUND_FIELD),
             rowann.getAs[Int](BEST_GUESS_FIELD),
             rowann.getAs[Int](UPPER_BOUND_FIELD),
           ),
           attributes.schema
                     .fields
                     .map { _.name }
                     .map { name =>
                       val r = attributes.getAs[Row](name)
                       name ->
                       (
                         r.getAs[String](LOWER_BOUND_FIELD),
                         r.getAs[String](UPPER_BOUND_FIELD)
                       )
                     }
                     .toMap
         )
       }
    )
  }


  def ones(): (Int,Int,Int) = (1,1,1)

  def repeat[T](el: T, cnt: Int): Seq[T] = {
    (1 to cnt).map { x => el }
  }

  "DataFrame Range Annotations" >> {

    "support simple operators without caveats" >> {
      annotate(
        dfr.select()
        // , trace = true
      ) { result =>
        result.map { _._1 } must be equalTo(repeat(ones, 7))
      }

      annotate(
        dfr.filter { $"A" =!= 1 }
          // , trace = true
      ) { result =>
          result.map { _._1 } must be equalTo(repeat(ones, 3))
      }

      annotate(
        dfr.select($"A", $"B").join(dfs.filter($"D" === 2), $"A" === $"D")
          // , trace = true
      ) { result =>
          result.map { _._1 } must be equalTo(repeat(ones, 4))
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
      annotate(
        dftip.uncertainToAnnotation(
          TupleIndependentProbabilisticDatabase("P"),
          CaveatRangeStrategy())
        // , trace = true
      ) { result =>
        result.map { _._1 } must be equalTo(
          Seq(
            (1,1,1),
            (0,1,1),
            (0,1,1),
            (0,0,1)
          )
        )
      }

      annotate(
        dftip.uncertainToAnnotation(
          TupleIndependentProbabilisticDatabase("P"),
          CaveatRangeStrategy()).filter( $"A" === 1 or $"A" === 3 )
        // , trace = true
      ) { result =>
        result.map { _._1 } must be equalTo(
          Seq(
            (1,1,1),
            (0,1,1),
            (0,0,1)
          )
        )
      }

      annotate(
        dftip.uncertainToAnnotation(
          TupleIndependentProbabilisticDatabase("P"),
          CaveatRangeStrategy())
          .select( $"A" ).join(
            dftip.uncertainToAnnotation(
              TupleIndependentProbabilisticDatabase("P"),
              CaveatRangeStrategy())
              .select( $"B"),
            $"A" === $"B"
          )
         // , trace = true
      ) { result =>
        result.map { _._1 } must be equalTo(
          Seq(
            (0,1,1),
            (0,1,1),
            (0,1,1),
            (0,0,1),
            (0,0,1)
          )
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
