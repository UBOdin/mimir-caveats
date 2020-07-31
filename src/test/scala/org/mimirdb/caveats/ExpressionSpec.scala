package org.mimirdb.caveats

import org.specs2.mutable.Specification

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.mimirdb.test._
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.annotate.CaveatExistsAttributeAnnotation

class ExpressionSpec
  extends Specification
  with ExpressionMatchers
  with SharedSparkTestInstance
{
  import spark.implicits._

  def row(fields:(Any, Boolean)*) = {
    InternalRow.fromSeq(
      fields.map { _._1 }
            .map { Literal(_).eval(InternalRow()) } ++
      fields.map { _._2 } ++
      Seq(false)
    )
  }

  def annotate[T](e: Column)(op: Expression => T): T =
  {
    val wrapper =
      dfr.select(e.as("TEST"))
        .trackCaveats
        .queryExecution
        .analyzed
        .children(0)
        // .children(0)
        .asInstanceOf[Project]
    // println(s"$e -> ${wrapper.treeString}")
    val schema =
      wrapper.child.output
    // println(s"SCHEMA: $schema")
    val testColumn =
      wrapper.output.find { _.name.equals("TEST") }.get
    val testAnnotationColumn =
      CaveatExistsAttributeAnnotation.annotationFor(testColumn)
    val result =
      wrapper
        .projectList
        .find { _.toAttribute.exprId.equals(testAnnotationColumn.exprId) }
        .get
        .children(0) // Strip off the Alias
        // .asInstanceOf[CreateNamedStruct]
        // .valExprs(1) // Caveats.COLUMN_ANNOTATION
        // .asInstanceOf[CreateNamedStruct]
        // .valExprs(0) // The only column we added
    // println(s"RESULT: $result")
    op( bindReference(result, schema) )
  }

  def test(e: Expression)(fields: (Any, Boolean)*): Boolean =
  {
    val ret = e.eval(row(fields:_*))
    ret.asInstanceOf[Boolean]
  }

  "AnnotateExpression" in {

    "handle simple caveat-free annotation" >> {


      annotate(lit(1)) { e =>
        // println(e)
        e must beEquivalentTo(Literal(false))
        test(e)() must beFalse
      }

      annotate($"A"){ e =>
        println(e)
        test(e)("1" -> false, "2" -> false, "3" -> false) must beFalse
        test(e)("1" -> false, "2" -> true,  "3" -> false) must beFalse
        test(e)("1" -> true,  "2" -> false, "3" -> false) must beTrue
      }

    }

    "handle simple annotation with caveats" >> {

      annotate(
        $"A".caveat("a possible error")
      ) { e =>
        test(e)("1" -> false, "2" -> false, "3" -> false) must beTrue
      }

    }

    "handle when clauses" >> {

      annotate(
        when($"B" === 0, $"A")
          .otherwise(1)
      ){ e =>
        test(e)("0" -> false, "1" -> false, "2" -> false) must beFalse
        test(e)("0" -> false, "1" -> true,  "2" -> false) must beTrue
        test(e)("0" -> false, "0" -> false, "2" -> false) must beFalse
        test(e)("0" -> false, "0" -> true,  "2" -> false) must beTrue
        test(e)("0" -> true,  "1" -> false, "2" -> false) must beFalse
        test(e)("0" -> true,  "0" -> false, "2" -> false) must beTrue
      }

      annotate(
        when($"B" === 0, $"A")
          .otherwise(lit(1).caveat("an error"))
      ){ e =>
        test(e)("0" -> false, "0" -> false, "2" -> false) must beFalse
        test(e)("0" -> false, "1" -> false, "2" -> false) must beTrue
      }
    }

    "handle conditional caveats" >> {

      annotate(
        $"A".caveatIf("A possible problem", $"A" === "1")
      ){
        e =>
        // No caveats = no caveats
        test(e)("0" -> false, "1" -> false, "2" -> false) must beFalse

        // Caveats on non-accessed values shouldn't change anything
        test(e)("0" -> false, "1" -> true,  "2" -> false) must beFalse

        // Caveats on the base value should propagate
        test(e)("0" -> true,  "1" -> false, "2" -> false) must beTrue

        // ... regardless of whether or not the condition is satisfied
        test(e)("1" -> true,  "1" -> false, "2" -> false) must beTrue

        // And the condition should apply caveats
        test(e)("1" -> false, "1" -> false, "2" -> false) must beTrue
      }

    }

    "handle conjunctions and disjunctions" >> {

      annotate(
        (($"A" === 1) and
          ($"B" === 1)) or
            ($"C" === 1)
      ){ e =>
        // no caveats
        test(e)("1" -> false, "1" -> false, "1" -> false) must beFalse

        // F OR T*
        test(e)("0" -> false, "1" -> false, "1" -> true)  must beTrue

        // T OR T*
        test(e)("1" -> false, "1" -> false, "1" -> true)  must beFalse

        // T* OR F
        test(e)("1" -> true, "1" -> false, "0" -> false)  must beTrue

        // T* OR T*
        test(e)("1" -> true, "1" -> false, "1" -> true)   must beTrue

        // T* AND T
        test(e)("1" -> true, "1" -> false, "0" -> false)  must beTrue

        // T* AND F
        test(e)("1" -> true, "0" -> false, "0" -> false)  must beFalse

        // T* AND T*
        test(e)("1" -> true, "1" -> true, "0" -> false)   must beTrue
      }
    }

    "handle UDFs" >> {
      val myudf = udf((x:String) => x)

      annotate(
        myudf($"A")
      ){ e =>
        // output will always be caveated
        test(e)("1" -> false, "1" -> false, "1" -> false) must beFalse

        // output will always be caveated
        test(e)("1" -> true, "1" -> true, "1" -> true) must beTrue
      }
    }

  }

}
