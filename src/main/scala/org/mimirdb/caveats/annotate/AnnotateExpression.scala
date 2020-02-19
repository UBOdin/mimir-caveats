package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  AggregateFunction,
  Sum
}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.mimirdb.caveats._
import org.mimirdb.spark.expressionLogic.{
  foldAnd, 
  foldOr, 
  negate, 
  aggregateBoolOr
}

/**
 * The [apply] method of this class is used to derive the annotation for an 
 * input expression.  Specifically, [apply] (and by extension [annotate]) 
 * returns an expression that, in the context of a [Row], will evaluate to true
 * if the input expression is Caveated on that row.
 * 
 * Spark allows turing complete logic in expressions, so determining whether an
 * expression is caveatted is intractable.  As a result, we adopt a conservative
 * approximation: If any input to the expression is caveatted, or if the 
 * expression includes a caveat, we default to assuming that the caveat applies 
 * to the entire expression.  In other words, if the annotation returns true, 
 * the input might be Caveated.  If the annotation returns fals,e the input is
 * definitely not Caveated.
 * 
 * A few development notes:
 *  - The annotation expression assumes that the [Row] on which it is evaluated
 *    was the result of a [LogicalPlan] that has been processed by 
 *    [AnnotatePlan] (i.e., there is a [Caveats.ANNOTATION_ATTRIBUTE] attribute).
 **/
class AnnotateExpression(
  pedantic: Boolean,
  expectAggregate: Boolean
)
  extends LazyLogging
{
  lazy val withoutExpectingAggregate = 
    new AnnotateExpression(pedantic = pedantic, expectAggregate = false)

  /**
   * Derive an expression to compute the annotation of the input expression
   * 
   * @param   expr  The expression to find the annotation of
   * @return        An expression that evaluates to true if expr is caveatted
   **/
  def apply(expr: Expression): Expression = 
  {
    // Derive a set of conditions under which  the input expression will be 
    // caveatted.  By default, this occurs whenever a caveat is present or when
    // an input attribute has a caveat.
    expr match {

      /////////////////////////////////////////////////////////////////////////
      // Part 1: Special cases
      //
      // For several expression types, we can do better than the naive default.
      /////////////////////////////////////////////////////////////////////////

      // The caveat expression (obviously) is always caveated.  If we're not
      // running in pedantic mode however, exclude global caveats.
      case caveat: ApplyCaveat => 
        if(pedantic || !caveat.global) { Literal(true) }
        else { apply(caveat.value) }

      // Attributes are caveatted if they are caveated in the input.
      //
      // Note: If we're expecting an aggregate, we can encounter loose 
      // attributes (e.g., part of the grouping expressions).  If so, the 
      // corresponding annotation lookup needs to be wrapped in an aggregate.
      case a: Attribute if expectAggregate => 
        aggregateBoolOr(Caveats.attributeAnnotationExpression(a.name))        

      case a: Attribute => 
        Caveats.attributeAnnotationExpression(a.name)

      // Not entirely sure how to go about handling subqueries yet
      case sq: SubqueryExpression => ???
      
      // If the predicate is guaranteed safe, we can limit ourselves to the
      // caveattedness of either the then or else clause.
      case If(predicate, thenClause, elseClause) => 

        // Two possible sources of taint: 'predicate' and the clauses.
        // Consider each case separately and then combine disjunctively.
        foldOr(Seq(

          // If 'predicate' is contaminated, it will contaminate the entire
          // expression.  
          apply(predicate), 

          // Otherwise, propagate the annotation from whichever branch ends
          // up being taken
          If(predicate, apply(thenClause), apply(elseClause))
        ):_*)

      // Similarly, if all preceding predicates of a CaseWhen are guaranteed 
      // safe, we can limit ourselves to the caveattedness of the corresponding 
      // branch.
      case CaseWhen(branches, otherwise) => {

        // This is basically the 'if' case on steroids.  The approach here is
        // pretty similar: We split every branch into two cases, taking 
        // advantage of the fact that branch clauses are evaluated in order.
        //  1. If the branch expression is caveated, we simply return 'true'
        //  2. If not, we evaluate the branch expression and if true, return
        //     the caveatedness of the outcome expression.  
        //  3. If the branch expression is false, we move on to the next branch.
        //  4. Once all branch expressions are exhausted, we return the 
        //     caveatedness of the otherwise clause.
        CaseWhen(

          // Case 3 is handled by falling through cases (i.e., the flat map)
          branches.flatMap { case (predicate, outcome) =>

            // Case 1 with a slight optimization: If we can guarantee no caveat, 
            // then we can safely skip this case.
            val predicateCaveatBranch:Option[(Expression,Expression)] = 
              apply(predicate) match { 
                case Literal(false, BooleanType) => None
                case predicateCaveat => Some( predicateCaveat -> Literal(true) )
              } 

            // Case 2, merged with the result of Case 1
            predicateCaveatBranch.toSeq :+ ( predicate -> apply(outcome) )
          },

          // Case 4 in the resulting otherwise clause
          otherwise.map { apply(_) }
        )

      }

      // For AND, we can fall through if either side is safely false.
      case And(lhs, rhs) => {
        val lhsCaveat = apply(lhs)
        val rhsCaveat = apply(rhs)
        foldOr(
          // Propagate when caveats appear on both sides
          foldAnd(lhsCaveat, rhsCaveat),
          // When RHS is true, propagate the LHS caveat
          foldAnd(lhsCaveat, rhs),
          // When LHS is true, propagate the RHS caveat
          foldAnd(rhsCaveat, lhs)
        )
      }

      // For OR, we can fall through if either side is safely true.
      case Or(lhs, rhs) => {
        val lhsCaveat = apply(lhs)
        val rhsCaveat = apply(rhs)
        foldOr(
          // Propagate when caveats appear on both sides
          foldAnd(lhsCaveat, rhsCaveat),
          // When RHS is false, propagate the LHS caveat
          foldAnd(lhsCaveat, negate(rhs)),
          // When LHS is false, propagate the RHS caveat
          foldAnd(rhsCaveat, negate(lhs))
        )
      }

      /////////////////////////////////////////////////////////////////////////
      // Part 2: Aggregates
      //
      // Spark logical plans are more closely tied to SQL syntax than to the
      // execution model, insofar as Aggregate() expressions are allowed to 
      // define both pre- and post- aggregation expressions.  That is, we're
      // allowed to have something like:
      // 
      //    SELECT sum(a+c)+sum(b+d) FROM ...
      // 
      // a+c get evaluated before the aggregation, and the two independent sums
      // get evaluated afterwards.  
      // 
      // AnnotateExpression is designed to handle such expressions as well.  
      // When an AggregateExpression appears here, the annotated result will 
      // also include an AggregateExpression that computes the corresponding
      // attribute annotation.
      /////////////////////////////////////////////////////////////////////////
      case AggregateExpression(
        aggFn, mode, isDistinct, resultId
      ) => 
      {
        // For now, we do something blatantly simple: The aggregate value is
        // caveated if:
        // 1. One of the aggregate function's arguments is caveated
        // 2. The filter (if present) is affected by a caveated value
        // 
        // This doesn't quite get us as tight a result as we could possibly get:
        // TODO: See if there are any aggregates that we can special-case
        // TODO: We might be able to get some added tightness from isDistinct
        // 
        // Another note: `mode` seems to be used mainly by the optimizer. 
        // See:
        // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/interfaces.scala
        // 
        // I'm not 100% certain, but I think the idea is to allow the optimizer
        // to rewrite the expression for partition-friendliness without needing
        // to muck around with the innards of aggFn... which internally defines
        // all of the aggregate logic.  As long as AnnotateExpression behaves
        // deterministically, this means that we should just be able to 
        // propagate the mode... and the other places in the tree where the
        // expression appears will just play nice.... I hope?
        //
        // TODO: Spark 3 appears to introduce a BoolOr aggregate.  Use that
        //       instead of the Sum + Comparison hack below
        if(!expectAggregate){
          throw new IllegalArgumentException("Unexpected aggregate within an aggregate")
        }
        val argumentAnnotations = aggFn.children.map { 
                                    withoutExpectingAggregate(_) 
                                  }
        val isCaveated = foldOr(argumentAnnotations:_*)
        aggregateBoolOr(isCaveated)
      }
      
      case _:AggregateFunction => 
        throw new IllegalArgumentException(
          "Expecting all `AggregateFunction`s to be nested within an AggregateExpression")

      //
      // We're not in one of our special cases.  
      case _ => foldOr(expr.children.map { apply(_) }:_*)
    }
  }

  def preserveName(expr: NamedExpression): NamedExpression =
  {
    Alias(apply(expr), expr.name)()
  }
}

object AnnotateExpression
{
  def apply(
    expr: Expression, 
    pedantic: Boolean = true, 
    expectAggregate: Boolean = false
  ): Expression =
  {
    new AnnotateExpression(
      pedantic = pedantic,
      expectAggregate = expectAggregate
    )(expr)
  }
}