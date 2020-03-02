package org.mimirdb.caveats.enumerate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.mimirdb.spark.expressionLogic.{
  foldAnd,
  foldOr,
  negate
}

object AggregateInteraction
  extends Enumeration
{
  type T = Value
  val IGNORE,
      OUTER_ONLY,
      INNER_ONLY = Value

  def test[Y](ai: T, inAgg: Boolean)(op: => Seq[Y]): Seq[Y] = 
    ai match { 
      case IGNORE => op
      case OUTER_ONLY if !inAgg => op
      case INNER_ONLY if inAgg => op
      case _ => Seq[Y]()
    }
}


class ExpressionDependency[T]
(
  detector: Expression => PartialFunction[Expression, T],
  aggregates: AggregateInteraction.T = AggregateInteraction.IGNORE
)
{
  /**
   * Recursively traverse the expression tree, building up a set of vSlices
   * under which the detected elements will be "triggered"
   */
  def apply(expr: Expression, vSlice: Expression, inAgg: Boolean): Seq[T] =
  {
    // current step
    AggregateInteraction.test[T](aggregates, inAgg) {
      detector(vSlice)
        .lift(expr)
        .toSeq 
    } ++ 
    // recursive step
    (expr match {
      case If(ifClause, thenClause, elseClause) => 
        return apply(ifClause, vSlice, inAgg) ++ 
                apply(thenClause, foldAnd(vSlice,ifClause), inAgg) ++
                apply(elseClause, foldAnd(vSlice,negate(ifClause)), inAgg)
      
      case CaseWhen(branches, otherwise) => {
        val (detectedList, finalSlice) = 
          branches.foldLeft(
            (Seq[T](), vSlice)
          ) { case ((detectedList: Seq[T], currentSlice:Expression), 
                    (branchSlice: Expression, branchResult: Expression)) => 
              (
                detectedList ++
                  apply(branchResult, foldAnd(currentSlice, branchSlice), inAgg),
                foldAnd(currentSlice, negate(branchSlice))
              )
          }

        return detectedList ++ otherwise.map { apply(_, finalSlice, inAgg) }
                                      .getOrElse { Seq() }
      }

      case AggregateExpression(fn, _, _, _) => apply(fn, vSlice, true)

      case _ => expr.children.flatMap { apply(_, vSlice, inAgg) } 

    })
  }
}

object ExpressionDependency
{

  def apply[T](
    expr: Expression, 
    vSlice: Expression = Literal(true),
    aggregates: AggregateInteraction.T = AggregateInteraction.IGNORE
  )(
    detector: Expression => PartialFunction[Expression, T]
  ) = new ExpressionDependency(detector, aggregates)(expr, vSlice, false)

  def attributes(
    expr: Expression,
    vSlice: Expression = Literal(true)
  ): Map[String, Expression] =
    apply[(String, Expression)](expr, vSlice) { vSlice => {
      case a:Attribute => a.name -> vSlice
    }}.groupBy { _._1 }
      .mapValues { elems => foldOr(elems.map { _._2 }:_*) }
  
}