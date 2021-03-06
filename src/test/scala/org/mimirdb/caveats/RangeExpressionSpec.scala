package org.mimirdb.caveats

import org.specs2.mutable.Specification

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

import org.mimirdb.test._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.annotate._

class RangeExpressionSpec
  extends Specification
  with ExpressionMatchers
  with SharedSparkTestInstance
  with DataFrameMatchers
{
  import spark.implicits._

  def fieldNames = Seq("a", "b", "c", "d", "e", "f", "g")

  def sparkDT (a: Any): DataType =
    a match {
      case x:Int => IntegerType
      case x:Double => DoubleType
      case x:String => StringType
      case x:Boolean => BooleanType
      case _ => StringType
    }

  def rangeRowDF(rowann:(Int,Int,Int), fields:(Any,Any,Any)*) : DataFrame =
  {
    // create a single row with the right annotation attribute
    val dt = sparkDT(fields(0)._2)
    val data = Seq(
      Row.fromSeq(
        fields.map { _._2 }
          ++ Seq(rowann._1, rowann._2, rowann._3)
          ++ fields.flatMap { case (lb,_,ub) => Seq(lb,ub) }
      )
    )
    val normalFields = fieldNames.slice(0, fields.length ).map(StructField(_, dt, true))

    val schema =
      StructType(
        normalFields ++
          CaveatRangeEncoding.annotationStruct(StructType(normalFields)).fields
      )
     val df = spark.createDataFrame(
       spark.sparkContext.parallelize(data),
       schema
     )
    df
  }

  def normalRow(fields:Any*) : DataFrame =
  {
    val dt = sparkDT(fields(0))
    val data = Seq(Row( fields:_* ))
    val schema = StructType(fieldNames.slice(0, fields.length).map(StructField(_, dt, true)))
    spark.createDataFrame(
       spark.sparkContext.parallelize(data),
       schema
     )
  }

  def cr[T](lb: T, bg: T, ub: T) : Column = {
    new Column(
      CaveatRange(
        value=Literal(bg),
        lb = Literal(lb),
        ub = Literal(ub),
        message = Literal("test case")
      )
    )
  }

  def inputAnnotRow(rowann:(Int,Int,Int), fields:(Any,Any,Any)*) : DataFrame =
  {
    // create a single row with the right annotation attribute
    val df = normalRow(fields.map(_._2):_*)
    val annDF = df.select(fields.zip(fieldNames).map( _ match { case (v,nam) => cr(v._1, v._2, v._3).as(nam) } ):_*)
    annDF
  }

  def dfEquals(left: DataFrame, right: DataFrame) : Boolean =
    (
      left.exceptAll(right)
        .union(
          right.exceptAll(left)
        )
    ).isEmpty

  def rangeTestCertInput(e: Column, trace: Boolean = false)
    (infields: Any*)
    (outfields: (Any,Any,Any)*)
      : Boolean =
    rangeTest(e, trace)((1,1,1), infields.map(x => (x,x,x)):_*)((1,1,1), outfields:_*)

  def rangeTest(e: Column, trace: Boolean = false)
    (inrowann: (Int,Int,Int), infields: (Any,Any,Any)*)
    (outrowann: (Int,Int,Int), outfields: (Any,Any,Any)*)
      : Boolean =
  {
    val indf = rangeRowDF(inrowann, infields:_*)
    val outdf = rangeRowDF(outrowann, outfields:_*)

    if (trace)
    {
      println(s"EXPRESSION: $e")
      println("INPUT DATA:")
      indf.show(1,100)
      println("EXPECTED OUTPUT DATA:")
      outdf.show(1,100)
    }

    val eresult = rangeAnnotate(e, indf, trace)

    if (trace)
    {
      println("ACTUAL OUTPUT DATA:")
      eresult.show(1,100)
      println("REWRITTTEN QUERY PLAN DATA:")
      println(eresult.queryExecution.analyzed)
    }
    tracePlan(outdf,trace)
    eresult must beBagEqualsTo(outdf)
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
      d.show(30,100)
    }
  }

  def rangeAnnotate[T](e: Column, input: DataFrame, trace: Boolean): DataFrame = {
    Caveats.annotate(input.select(e), CaveatRangeStrategy(), Constants.ANNOTATION_ATTRIBUTE, trace)
  }

  "RangeAnnotateExpression" in {
    import spark.implicits._

    "no caveats" >> {

      "literals" >> {
        rangeTest(lit(1).as("a"))((1,1,1),(1,1,1))((1,1,1),(1,1,1)) must beTrue
        rangeTest(lit(1).as("a"))((1,1,1),(0,2,3))((1,1,1),(1,1,1)) must beTrue
        // row annotations should propagate
        rangeTest(lit(1).as("a"))((1,2,3),(0,2,3))((1,2,3),(1,1,1)) must beTrue
      }

      "attribute references" >> {
        rangeTest($"a")((1,2,3),(4,5,6))((1,2,3),(4,5,6)) must beTrue
      }

      "arithmetic operations" >> {
        rangeTest(($"a" + $"b").as("a"))((1,1,1),(1,2,3),(3,3,10))((1,1,1),(4,5,13)) must beTrue
        rangeTest(($"a" * $"b").as("a"))((1,1,1),(-3,-2,-1),(3,3,10))((1,1,1),(-30,-6,-3)) must beTrue
        rangeTest(($"a" / $"b").as("a"))((1,1,1),(4,8,12),(2,4,4))((1,1,1),(1.0,2.0,6.0)) must beTrue
        rangeTest(($"a" - $"b").as("a"))((1,1,1),(12,14,15),(3,3,10))((1,1,1),(2,11,12)) must beTrue
      }

      "comparisons" >> {
        rangeTest(($"a" === $"b").as("a"))((1,1,1),(1,2,3),(2,2,4))((1,1,1),(false,true,true)) must beTrue
        rangeTest(($"a" < $"b").as("a"))((1,1,1),(1,2,3),(1,3,3))((1,1,1),(false,true,true)) must beTrue
      }

      "boolean logic" >> {
        // or
        rangeTest(($"a" or $"b").as("a"))((1,1,1),(false,false,true),(true,true,true))((1,1,1),(true,true,true)) must beTrue
        rangeTest(($"a" or $"b").as("a"))((1,1,1),(false,false,true),(false,false,true))((1,1,1),(false,false,true)) must beTrue

        // and
        rangeTest(($"a" and $"b").as("a"))((1,1,1),(false,false,true),(true,true,true))((1,1,1),(false,false,true)) must beTrue

        // not
        rangeTest((!($"a")).as("a"))((1,1,1),(false,false,true))((1,1,1),(false,true,true)) must beTrue
        rangeTest((!($"a")).as("a"))((1,1,1),(true,true,true))((1,1,1),(false,false,false)) must beTrue
        rangeTest((!($"a")).as("a"))((1,1,1),(false,false,false))((1,1,1),(true,true,true)) must beTrue
      }

      "conditionals" >> {
        // certainly true -> return c
        rangeTest((when($"a" === $"b",$"c").otherwise($"d")).as("a"))((1,1,1),(1,1,1),(1,1,1),(1,2,10),(4,4,5))((1,1,1),(1,2,10)) must beTrue
        // certainly false -> return d
        rangeTest((when($"a" === $"b",$"c").otherwise($"d")).as("a"))((1,1,1),(1,1,1),(2,2,2),(1,2,10),(4,4,5))((1,1,1),(4,4,5)) must beTrue
        // possibly and best guess true -> return min(c,d), max(c,d)
        rangeTest((when($"a" === $"b",$"c").otherwise($"d")).as("a"))((1,1,1),(1,1,1),(1,2,2),(1,2,10),(4,4,15))((1,1,1),(1,4,15)) must beTrue
        // two when cases
        rangeTest((when($"a" === $"b",$"c").when($"b" === 10, 20).otherwise($"d").as("a")))((1,1,1),(1,1,1),(1,9,9),(1,2,10),(4,4,15))((1,1,1),(1,4,15))
      }
    }

    "caveats" >> {

      "literals" >> {
        rangeTestCertInput(lit(1).rangeCaveat("oh my!",lit(0),lit(2)).as("a"))(1)((0,1,2)) must beTrue
        rangeTestCertInput(lit(1).rangeCaveatIf("oh my!",lit(0),lit(2),$"a" === 1).as("a"))(1)((0,1,2)) must beTrue
        rangeTestCertInput(lit(1).rangeCaveatIf("oh my!",lit(0),lit(2),$"a" === 2).as("a"))(1)((1,1,1)) must beTrue
      }

      "attribute references" >> {
        rangeTestCertInput($"a".rangeCaveat("oh my!",lit(0),lit(2)).as("a"))(1)((0,1,2)) must beTrue
        rangeTestCertInput($"a".rangeCaveatIf("oh my!",lit(0),lit(2),$"a" === 1).as("a"))(1)((0,1,2)) must beTrue
        rangeTestCertInput($"a".rangeCaveatIf("oh my!",lit(0),lit(2),$"a" === 2).as("a"))(1)((1,1,1)) must beTrue
      }

      "arithmetic operations" >> {
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(0),lit(2)) + $"b".rangeCaveat("oh my!",lit(1),lit(2)))
          .as("a"))(1,2)((1,3,4)) must beTrue
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(-3),lit(-1)) * $"b".rangeCaveat("oh my!",lit(3),lit(10)))
          .as("a"))(-2,3)((-30,-6,-3)) must beTrue
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(4.0),lit(12.0)) / $"b".rangeCaveat("oh my!",lit(2.0),lit(4.0)))
          .as("a"))((8.0),(4.0))((1.0,2.0,6.0)) must beTrue
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(12),lit(15)) - $"b".rangeCaveat("oh my!",lit(3),lit(10)))
          .as("a"))(14,3)((2,11,12)) must beTrue
      }

      "comparisons" >> {
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(1),lit(3)) === $"b".rangeCaveat("oh my!",lit(1),lit(3))).as("a"))(2,2)((false,true,true)) must beTrue
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(1),lit(3)) < $"b".rangeCaveat("oh my!",lit(1),lit(3))).as("a"))(2,3)((false,true,true)) must beTrue
      }

      "boolean logic" >> {
        // or
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(false),lit(true)) or $"b".rangeCaveat("oh my!",lit(true),lit(true))).as("a")
        )(false,true)((true,true,true)) must beTrue
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(false),lit(true)) or $"b".rangeCaveat("oh my!",lit(false),lit(true))).as("a")
        )(false,false)((false,false,true)) must beTrue

        // and
        rangeTestCertInput(($"a".rangeCaveat("oh my!",lit(false),lit(true)) and $"b".rangeCaveat("oh my!",lit(true),lit(true))).as("a")
        )(false,true)((false,false,true)) must beTrue

        // not
        rangeTestCertInput((!($"a".rangeCaveat("oh my!",lit(false),lit(true)))).as("a"))(false)((false,true,true)) must beTrue
        rangeTestCertInput((!($"a".rangeCaveat("oh my!",lit(true),lit(true)))).as("a"))(true)((false,false,false)) must beTrue
        rangeTestCertInput((!($"a".rangeCaveat("oh my!",lit(false),lit(false)))).as("a"))(false)((true,true,true)) must beTrue
      }

      "conditionals" >> {
        // certainly true -> return c
        rangeTestCertInput((when($"a".rangeCaveat("oh my!",lit(1),lit(1)) === $"b".rangeCaveat("oh my!",lit(1),lit(1)),
          $"c".rangeCaveat("oh my!",lit(1),lit(10))).otherwise($"d".rangeCaveat("oh my!",lit(4),lit(5)))).as("a")
//          , trace = true
        )(1,1,2,4)((1,2,10)) must beTrue
        // // certainly false -> return d
        rangeTestCertInput((when($"a".rangeCaveat("oh my!",lit(1),lit(1)) === $"b".rangeCaveat("oh my!",lit(2),lit(2)),
        $"c".rangeCaveat("oh my!",lit(1),lit(10))).otherwise($"d".rangeCaveat("oh my!",lit(4),lit(5)))).as("a")
          // , trace = true
        )(1,2,2,4)((4,4,5)) must beTrue
        // // possibly and best guess true -> return min(c,d), max(c,d)
        rangeTestCertInput((when($"a".rangeCaveat("oh my!",lit(1),lit(1)) === $"b".rangeCaveat("oh my!",lit(1),lit(2)),
          $"c".rangeCaveat("oh my!",lit(1),lit(10))).otherwise($"d".rangeCaveat("oh my!",lit(4),lit(15)))).as("a")
        )(1,2,2,4)((1,4,15)) must beTrue
        // // two when cases
        rangeTestCertInput((when($"a".rangeCaveat("oh my!",lit(1),lit(1)) === $"b".rangeCaveat("oh my!",lit(1),lit(9)),
          $"c".rangeCaveat("oh my!",lit(1),lit(10))).otherwise($"d".rangeCaveat("oh my!",lit(4),lit(15)))).as("a")
        )(1,9,2,4)((1,4,15))
      }

      "udfs" >> {
        val myudf = udf((x:Int) => x)

        rangeTestCertInput(myudf($"a").as("a"),
//          trace = true
        )(1,1,1,1)((-2147483648,1,2147483647)) must beTrue
      }
    }

  }

}
