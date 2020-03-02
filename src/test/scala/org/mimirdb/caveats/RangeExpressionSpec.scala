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

import org.mimirdb.implicits._

import org.mimirdb.caveats.annotate._

class RangeExpressionSpec
  extends Specification
  with ExpressionMatchers
  with SharedSparkTestInstance
{
  import spark.implicits._

  def fieldNames = Seq("a", "b", "c", "d", "e", "f", "g")

  def sparkDT (a: Any): DataType =
    a match {
      case x:Int => IntegerType
      case x:String => StringType
      case x:Boolean => BooleanType
      case _ => StringType
    }

  def rangeRowDF(rowann:(Int,Int,Int), fields:(Any,Any,Any)*) : DataFrame =
  {
    // create a single row with the right annotation attribute
    val dt = sparkDT(fields(0)._2)
    val data =
    Seq(
      Row(
        fields.map { _._2 }
          :+
          Row(
            Row(rowann._1, rowann._2, rowann._3),
            Row.fromSeq(
              fields.map { case (lb,_,ub) => Row(lb, ub) }
            )
          )
        :_*
      )
    )
    val normalFields = fieldNames.slice(0, fields.length ).map(StructField(_, dt, true))
    val schema =
      StructType(
        normalFields :+
          StructField(Constants.ANNOTATION_ATTRIBUTE,
            CaveatRangeEncoding.annotationStruct(
              StructType(normalFields)
            )
          )
      )
     spark.createDataFrame(
       spark.sparkContext.parallelize(data),
       schema
     )
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
    annDF.show(1,100)
    annDF
  }

  def dfEquals(left: DataFrame, right: DataFrame) : Boolean =
    (
      left.exceptAll(right)
        .union(
          right.exceptAll(left)
        )
    ).isEmpty

  def rangeTest(e: Column)
    (inrowann: (Int,Int,Int), infields: (Any,Any,Any)*)
    (outrowann: (Int,Int,Int), outfields: (Any,Any,Any)*)
      : Boolean =
  {
    val indf = rangeRowDF(inrowann, infields:_*)
    // indf.show(1,100)
    val outdf = rangeRowDF(outrowann, outfields:_*)
    // outdf.show(1,100)
    val eresult = rangeAnnotate(e, indf)
    // eresult.show(1,100)
    dfEquals(eresult, outdf)
  }

  def rangeAnnotate[T](e: Column, input: DataFrame): DataFrame = input.select(e).rangeCaveats

  "RangeAnnotateExpression" in {
    import spark.implicits._

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
      rangeTest(($"a" / $"b").as("a"))((1,1,1),(4,8,12),(2,4,4))((1,1,1),(1,2,6)) must beTrue
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

}
