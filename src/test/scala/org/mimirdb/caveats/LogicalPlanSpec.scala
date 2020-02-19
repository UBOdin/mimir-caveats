package org.mimirdb.caveats

import org.specs2.mutable.Specification

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ SparkSession, DataFrame, Column, Row }
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.Constants._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.annotate._

class LogicalPlanSpec 
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

  def annotate[T](input: DataFrame, trace: Boolean = false)( op : Seq[(Boolean, Map[String,Boolean])] => T) =
  {
    val annotated = Caveats.annotate(input, CaveatExists(trace = trace))
    if(trace){
      println("------ FINAL ------")
      println(annotated.queryExecution.analyzed)
    }
    op(
      annotated
       .collect()
       .map { row =>
         val annotation = row.getAs[Row](ANNOTATION_ATTRIBUTE)
         (
           annotation.getAs[Boolean](ROW_FIELD),
           annotation.schema
                     .fields
                     .map { _.name }
                     .map { name => name -> annotation.getAs[Boolean](name) }
                     .toMap
         )
       }
    )
  }

  "DataFrame Annotations" >> {

    "support simple operators without caveats" >> {
      annotate(
        df.select()
      ) { annotations => 
        annotations.map { _._1 } must be equalTo(Seq(false, false, false, false, false, false, false))
      }

      annotate(
        df.filter { $"A" =!= 1 }
      ) { annotations => 
          annotations.map { _._1 } must be equalTo(Seq(false, false, false))
      }
    }

    "support aggregates without caveats" >> {
      annotate(
        df.select( sum($"A") )
      ) { annotations =>
        annotations.map { _._1 } must be equalTo(Seq(false))
      }
      annotate(
        df.select( $"A", $"B".cast("int").as("B") )
          .groupBy($"A").sum("B")
      ) { annotations =>
        annotations.map { _._1 } must be equalTo(Seq(false, false, false))
      }
    }

  }
}
