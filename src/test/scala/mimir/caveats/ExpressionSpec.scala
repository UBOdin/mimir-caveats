package mimir.caveats

import org.specs2.mutable.Specification
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.specs2.matcher.Matcher
import org.apache.spark.sql.types._


import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

class ExpressionSpec extends Specification 
{

  lazy val spark = 
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      .getOrCreate()
  lazy val testData = /* R(A int, B int, C int) */
    spark.read
         .format("csv")
         .option("header", "true")
         .load("test_data/r.csv")

  def resolve(e: Column, df: DataFrame = testData) =
    df.select(e)
      .queryExecution
      .analyzed
      .asInstanceOf[Project]
      .projectList(0)

  def attr(e: String, df: DataFrame = testData): Attribute =
    resolve(col(e), df).asInstanceOf[Attribute]

  def cast(e: Expression, t: DataType): Expression = 
    Cast(e, t, Some(java.time.ZoneId.systemDefault.toString))

  def attrAnnotation(attribute:String, df: DataFrame = testData) = 
    Caveats.attributeAnnotationExpression(
      attr(attribute, df).asInstanceOf[Attribute]
    )

  def annotate(e: Column, df: DataFrame = testData): Expression =
    AnnotateExpression( resolve(e, df) )

  def recursiveCmp(e1: Expression, e2: Expression): Seq[String] =
  {
    if(e1.semanticEquals(e2)){ return Seq() }
    if(e1.children.length != e2.children.length) {
      Seq(
        "Differing numbers of children:",
        s"  ${e1.children.length} children: $e1",
        s"  ${e2.children.length} children: $e2"
      )
    } else {
      val childCmp = 
        e1.children.zip(e2.children).flatMap { case (child1, child2) => recursiveCmp(child1, child2) }
      if(childCmp.isEmpty) {
        def decorate(e: Expression): String = {
          (Seq(e.getClass.toString) ++ (e match {
            case Cast(_, _, tz) => Seq(s"Timezone: $tz")
            case _ => Seq()
          })).mkString(", ")
        }

        Seq(
          s"Got:      $e1 (${decorate(e1)})",
          s"Expected: $e2 (${decorate(e2)})"
        )
      } else { childCmp }
    }
  }

  def beEquivalentTo(cmp:Expression): Matcher[Expression] = { e: Expression =>
    (e.semanticEquals(cmp), s"$e !≅ $cmp\n${recursiveCmp(e, cmp).map { "  "+_ }.mkString("\n")}")
  }

  "AnnotateExpression" in {

    "handle simple caveat-free annotation" >> {
      import spark.implicits._

      annotate(lit(1)) must beEquivalentTo(Literal(false))
      annotate($"A") must beEquivalentTo(attrAnnotation("A"))
    }

    "handle when clauses" >> {
      import spark.implicits._

      annotate( 
        when(lit(0) === 0, $"A")
          .otherwise(1) 
      ) must beEquivalentTo(
        CaseWhen(Seq(
            EqualTo(Literal(0), Literal(0)) -> attrAnnotation("A")
          ), Literal(false)
        )
      )

      annotate( 
        when($"B" === 0, $"A")
          .otherwise(1) 
      ) must beEquivalentTo(
        CaseWhen(Seq(
            attrAnnotation("B") -> Literal(true),
            EqualTo(cast(attr("B"), IntegerType), Literal(0)) -> attrAnnotation("A")
          ), Literal(false)
        )
      )
    }


  }

}