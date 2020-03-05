package org.mimirdb.test

import org.apache.spark.sql.catalyst.expressions._
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchersImplicits._

trait ExpressionMatchers
{
  def recursiveCmp(e1: Expression, e2: Expression): Seq[String] =
  {
    if(e1.semanticEquals(e2)){ return Seq() }
    if(e1.children.length != e2.children.length) {
      Seq(
        s"Got:      ${e1.children.length} children in $e1",
        s"Expected: ${e2.children.length} children in $e2"
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
}