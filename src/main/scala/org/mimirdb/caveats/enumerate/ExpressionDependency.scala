package org.mimirdb.caveats.enumerate

import org.apache.spark.sql.catalyst.expressions._
import org.mimirdb.spark.expressionLogic.{
  foldAnd,
  foldOr,
  negate
}


class ExpressionDependency[T]
(
  detector: Expression => PartialFunction[Expression, T]
)
{
  /**
   * Recursively traverse the expression tree, building up a set of conditions
   * under which the detected elements will be "triggered"
   */
  def apply(expr: Expression, condition: Expression): Seq[T] =
  {
    // current step
    detector(condition)
      .lift(expr)
      .toSeq ++ 
    // recursive step
    (expr match {
      case If(ifClause, thenClause, elseClause) => 
        return apply(ifClause, condition) ++ 
                apply(thenClause, foldAnd(condition,ifClause)) ++
                apply(elseClause, foldAnd(condition,negate(ifClause)))
      
      case CaseWhen(branches, otherwise) => {
        val (detectedList, finalCondition) = 
          branches.foldLeft(
            (Seq[T](), condition)
          ) { case ((detectedList: Seq[T], currentCondition:Expression), 
                    (branchCondition: Expression, branchResult: Expression)) => 
              (
                detectedList ++
                  apply(branchResult, foldAnd(currentCondition, branchCondition)),
                foldAnd(currentCondition, negate(branchCondition))
              )
          }

        return detectedList ++ otherwise.map { apply(_, finalCondition) }
                                      .getOrElse { Seq() }
      }

      case _ => expr.children.flatMap { apply(_, condition) } 

    })
  }
}

object ExpressionDependency
{

  def apply[T](
    expr: Expression, 
    condition: Expression = Literal(true)
  )(
    detector: Expression => PartialFunction[Expression, T]
  ) = new ExpressionDependency(detector)(expr, condition)

  def attributes(expr: Expression): Map[String, Expression] =
    apply[(String, Expression)](expr) { condition => {
      case a:Attribute => a.name -> condition
    }}.groupBy { _._1 }
      .mapValues { elems => foldOr(elems.map { _._2 }:_*) }
  
}