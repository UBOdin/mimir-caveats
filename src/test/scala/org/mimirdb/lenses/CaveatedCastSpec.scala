package org.mimirdb.lenses

import org.specs2.mutable.Specification
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.mimirdb.lenses.implicits._
import org.mimirdb.test._
import org.mimirdb.caveats.ApplyCaveat

class CaveatedCastSpec 
  extends Specification
{

  def hasCaveats(e: Expression): Boolean = 
  {
    e match {
      case c:ApplyCaveat if c.condition.eval().asInstanceOf[Boolean] => true
      case _ => e.children.exists { hasCaveats(_) }
    }
  }

  def hasApplyCaveat(e: Expression): Boolean =
  {
    e match {
      case _:ApplyCaveat => true
      case _ => e.children.exists { hasApplyCaveat(_) }
    }
  }

  def tryCast[T](e: String, t: DataType)(op:(Any, Boolean) => T): T =
    tryCast(lit(e), t)(op)
  def tryCast[T](e: Column, t: DataType)(op:(Any, Boolean) => T): T =
  {
    val c = e.castWithCaveat(t)
    op(
      c.expr.eval(),
      hasCaveats(c.expr)
    )
  }

  "CaveatedCast to Int" >> {
    tryCast("3", IntegerType) { (value, caveat) => 
      caveat must beFalse
      value must beEqualTo(3)
    }
    tryCast("3vent", IntegerType) { (value, caveat) => 
      caveat must beTrue
      value must beNull
    }
  }

  "CaveatedCast to String" >> {
    tryCast(lit(3), StringType) { (value, caveat) => 
      caveat must beFalse
      value.toString must beEqualTo("3")
    }
  }

  "Optimize Freebies" >>
  {
    hasApplyCaveat(lit(3).castWithCaveat(LongType).expr) must beFalse
    hasApplyCaveat(lit(3).castWithCaveat(StringType).expr) must beFalse
    hasApplyCaveat(lit(3l).castWithCaveat(IntegerType).expr) must beTrue
  }

  "Ignore Incoming Nulls" >>
  {
    tryCast(lit(null), StringType) { (value, caveat) =>
      value must beNull
      caveat must beFalse
    }
    tryCast(lit(null).cast("int"), StringType) { (value, caveat) => 
      value must beNull
      caveat must beFalse
    }
  }


}