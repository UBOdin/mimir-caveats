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


import org.mimirdb.caveats.implicits._

class ExpressionSpec 
  extends Specification 
  with ExpressionMatchers
  with SharedSparkTestInstance
{
  import spark.implicits._

  def row(fields:(Any, Boolean)*) = {
    InternalRow.fromSeq(
      fields.map { _._1 }
            .map { Literal(_).eval(InternalRow()) } :+
      InternalRow(
        false,
        InternalRow(fields.map { _._2 }:_*)
      )
    )
  }

  def annotate[T](e: Column)(op: Expression => T): T =
  {
    val wrapper =
      df.select(e)
        .annotate
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
    val schema = 
      wrapper.child.output
    val result =
      wrapper
        .projectList
        .find { _.name.equals(Constants.ANNOTATION_ATTRIBUTE) }
        .get
        .children(0) // Strip off the Alias
        .asInstanceOf[CreateNamedStruct]
        .valExprs(1) // Caveats.COLUMN_ANNOTATION
        .asInstanceOf[CreateNamedStruct]
        .valExprs(0) // The only column we added
    
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
        e must beEquivalentTo(Literal(false))
        test(e)() must beFalse
      }

      annotate($"A"){ e =>
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


  }

}