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
                  apply(branchSlice, currentSlice, inAgg) ++
                  apply(branchResult, foldAnd(currentSlice, branchSlice), inAgg),
                foldAnd(currentSlice, negate(branchSlice))
              )
          }

        return detectedList ++ otherwise.map { apply(_, finalSlice, inAgg) }
                                      .getOrElse { Seq() }
      }

      case AggregateExpression(fn, _, _, _, _) => apply(fn, vSlice, true)

      case _ => expr.children.flatMap { apply(_, vSlice, inAgg) } 

    })
  }
}

object ExpressionDependency
{

  /**
   * Collect data based on the dependencies of an expression.
   * @param  expr       The expression to check for attribute dependencies
   * @param  vSlice     The slice under which the dependency exists
   * @param  aggregates How to interact with aggregate functions
   * @param  detector   The collection rule visitor (slice => visitedExpr => output)
   * @return            A slice (a map of exprID -> condition)
   *
   * A slice indicates some (potentially non-contiguous) region of a 
   * dataframe.  A slice is normally given as Map of column/expression 
   * pairs, where the expression indicates the subset of rows for which
   * the given column is to be indicated.
   * 
   * apply() starts with the default slice given in vSlice, but 
   * refines it as it iterates through the expression.  For example, 
   * given the expression `A OR B`, the slice when visiting `A` will
   * be `vSlice OR NOT B`.
   * 
   * The dectector is a partial function indicating which expressions
   * need to be collected, and the left hand side input is the slice.
   */
  def apply[T](
    expr: Expression, 
    vSlice: Expression = Literal(true),
    aggregates: AggregateInteraction.T = AggregateInteraction.IGNORE
  )(
    detector: Expression => PartialFunction[Expression, T]
  ) = new ExpressionDependency(detector, aggregates)(expr, vSlice, false)

  /**
   * Return the attributes on which a given expression depends, coupled
   * with the conditions under which the dependency exists.
   * @param  expr       The expression to check for attribute dependencies
   * @param  vSlice     The slice under which the dependency exists
   * @return            A slice (a map of exprID -> condition)
   *
   * A slice indicates some (potentially non-contiguous) region of a 
   * dataframe.  A slice is normally given as Map of column/expression 
   * pairs, where the expression indicates the subset of rows for which
   * the given column is to be indicated.
   */
  def attributes(
    expr: Expression,
    vSlice: Expression = Literal(true)
  ): Map[ExprId, Expression] =
    apply[(ExprId, Expression)](expr, vSlice) { vSlice => {
      case a:Attribute => a.exprId -> vSlice
    }}.groupBy { _._1 }
      .mapValues { elems => foldOr(elems.map { _._2 }:_*) }
  
}