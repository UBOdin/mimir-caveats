package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.mimirdb.caveats._

/**
 * The [apply] method of this class is used to derive the annotation for an
 * input expression.  Specifically, [apply] (and by extension [annotate])
 * returns a pair of expressions that, in the context of a [Row], will evaluate
 * to a lower and an upper bound on the value of an expression acroess all
 * possible worlds based on such bounds for the inputs of the expression.
 *
 * The semantics is that as long only expressions that are supported by this
 * approach are used and as long as the range annotations on input values are
 * bounding the input values, then the range annotations on output values
 * computed by the pair of expressions that is returend bound the output of the
 * expression. Note that these bounds may not be tight since computing tight
 * bounds is often intractable.
 *
 * A few development notes:
 *  - The annotation expression assumes that the [Row] on which it is evaluated
 *    was the result of a [LogicalPlan] that has been processed by
 *    [CaveatRangePlan] (i.e., there is a [Caveats.ANNOTATION] column which stores
 *    bounds for input values of expressions).
 * TODO:
 * - we assume that least/greatest is supported, we need to have an indicator
 *   which datatypes support these and otherwise fall back to boolean
 *   annotations for types that do not
 * - how to deal with null values? They do not fall into the domain so nullable
 *   answer can mess up LB < BG < UB. We should probably deal with nulls by
 *   applying a V-table labeling scheme before doing range-annotated expression
 *   evaluation. However, we still have to deal with nulls that are introduced by
 *   expression evalation, e.g., a CASE WHEN without ELSE where all predicates
 *   evaluate to false returns null. Alternatively we need to check for null and
 *   degreate the range whenever one of the values could be null.
 */
object CaveatRangeExpression
  extends LazyLogging
{
  /**
   * Derive an expression to compute the annotation of the input expression
   *
   * @param   expr  The expression to calculate bounds for
   * @return        A triple of expressions that computes lower, best guess, and upper bounds of the value of the input expression
   **/
  def apply(expr: Expression): (Expression, Expression, Expression) =
  {
    // Derive a set of conditions under which  the input expression will be
    // caveatted.  By default, this occurs whenever a caveat is present or when
    // an input attribute has a caveat.
    expr match {

      // Access value and bounds stored in Caveat
      case caveat: CaveatRange => (caveat.lb, caveat.value, caveat.ub)

      // the bounds of a Literal are itself
      case l: Literal => (l,l,l)

      // Attributes are caveatted if they are caveated in the input.
      case a: Attribute => (RangeCaveats.lbExpression(RangeCaveats.attributeAnnotationExpression(a)),
        RangeCaveats.bgAttrExpression(a),
        RangeCaveats.ubExpression(RangeCaveats.attributeAnnotationExpression(a))
      )

      // Not entirely sure how to go about handling subqueries yet
      case sq: SubqueryExpression => throw new AnnotationException("Can't handle subqueries yet", expr)

      // Arithemic
      case Add(left,right) => liftPointwise(expr)

      case Subtract(left,right) => {
        val (llb, lbg, lub) = apply(left)
        val (rlb, rbg, rub) = apply(right)

        (Subtract(llb,rub),expr,Subtract(rub,llb))
      }

      case Multiply(left,right) => minMaxAcrossAllCombinations(Multiply(left,right))

      case Divide(left,right) =>  minMaxAcrossAllCombinations(Divide(left,right))

      // conditional
      case If(predicate, thenClause, elseClause) =>
        {
          val cav_predicate = apply(predicate)
          val cav_then = apply(thenClause)
          val cav_else = apply(elseClause)
          val certainTrue = isCertainTrue(cav_predicate)
          val certainFalse = isCertainFalse(cav_predicate)

          //TODO for certain conditions we can omit the third clause. Add inference for detecting whether an expression's value will be certain
          (
            // If certainly true or false then only the then or else clause matters
            // otherwise we have to take the minimum or maximum over then and else
            CaseWhen(
              Seq(
                // check for certainly true, if yes then return then lower bound
                (certainTrue, getLB(cav_then)),
                // check for certainly false, if yes then return else lower bound
                (certainFalse, getLB(cav_else))
              ),
              // if predicate result is uncertain the take the minimum of then and else lower bounds
              Least(Seq(getLB(cav_then), getLB(cav_else)))
            ),
            expr,
            // symmetric to lower bound
            CaseWhen(
              Seq(
                // check for certainly true, if yes then return then upper bound
                (certainTrue, getUB(cav_then)),
                // check for certainly false, if yes then return else upper bound
                (certainFalse, getUB(cav_else))
              ),
              // if predicate result is uncertain the take the maximum of then and else upper bounds
               Greatest(Seq(getUB(cav_then), getUB(cav_else)))
            )
          )
        }

        // We have to compute the min / max across all outcomes for all clauses
        // The only exception is if for a prefix of clauses all predicates
        // evaluate to certainly false followed by one clause predicate which is
        // certainly true (or there is an else clause). We have to test for each
        // such prefix. We do this by nesting, e.g., if the first clause's predicate
        // is certainly false, then we use another level of CaseWhen to test whether
        // the next predicate is certainly false or true. This way we avoid potentially
        // an factor n (number of clauses) blow-up in the size of predicates
      case CaseWhen(branches, otherwise) => whenCases(CaseWhen(branches, otherwise))

      //   val casesB = whenCases(branches :: els)
      //   (
      //     casesB._1,
      //     CaseWhen(branches, otherwise),
      //     casesB._2
      //   )
      // }

      case Not(child) => {
        val (lb,bg,ub) = apply(child)
        (Not(ub), Not(bg), Not(lb))
      }

      case And(lhs, rhs) => liftPointwise(expr)

      case Or(lhs, rhs) => liftPointwise(expr)

      // works for and / or /
      case _ => liftPointwise(expr)
    }
  }

  //TODO ok to duplicate names?
  def preserveName(expr: NamedExpression): (NamedExpression,NamedExpression,NamedExpression) =
  {
    val (lb,bg,ub) = apply(expr)
    (Alias(lb, expr.name)(), Alias(bg, expr.name)(), Alias(ub, expr.name)())
  }

  // map a triple of boooleans into a triple of Int (Row annotations)
  def booleanToRowAnnotation(cond: (Expression, Expression, Expression)): (Expression, Expression, Expression) =
  {
    cond match { case (lb,bg,ub) =>
      (boolToInt(lb), boolToInt(bg), boolToInt(ub))
    }
  }

  private def boolToInt(bool: Expression): Expression = {
    If(EqualTo(bool, Literal(true)), Literal(1), Literal(0))
  }

  // recursive function to deal with CASE WHEN clauses. For CASE WHEN (p1 o1) (p2 o2) ... e,
  // we calculate the lower bound (symmetrically the upper bound) as
  // CASE WHEN (certaintrue(p1) LB(o1))
  //           (certainfalse(p1) (CASE WHEN (certaintrue(p2) LB(o2))
  //                              ...
  //           least(LB(o1), (CASE WHEN (certaintrue(p2) ...
  private def whenCases(when: CaseWhen): (Expression, Expression, Expression) =
    when match {
      case CaseWhen(Seq(), None) => null
      // a single when then clause
      case CaseWhen(Seq((pred, outcome)), None) => {
            val predB = apply(pred)
            val outcomeB = apply(outcome)

            (
              CaseWhen(
                Seq(
                  (isCertainTrue(predB), getLB(predB))
                )
                //TODO need a minimum domain value here (now this will be null)
              ),
              when,
              CaseWhen(
                Seq(
                  (isCertainTrue(predB), getUB(predB))
                )
                //TODO need to plug in maximum domain value here (now this will be null)
              )
            )
          }
       // an else clause only
      case CaseWhen(Seq(), Some(els)) => {
            val elsB = apply(els)
            ( getLB(elsB), els, getUB(elsB) )
          }
      case CaseWhen((pred, outcome) +: otherClauses, els) => {
        val otherBounds = whenCases(CaseWhen(otherClauses, els))
        val predB = apply(pred)
        val outcomeB = apply(outcome)

        (
          CaseWhen(
            Seq(
              (isCertainTrue(predB), getLB(predB)),
              (isCertainFalse(predB), getLB(otherBounds))
            ),
            Least(Seq(getLB(outcomeB), getLB(otherBounds)))
          ),
          when,
          CaseWhen(
            Seq(
              (isCertainTrue(predB), getUB(predB)),
              (isCertainFalse(predB), getUB(otherBounds))
            ),
            Greatest(Seq(getUB(outcomeB), getUB(otherBounds)))
          )
        )
      }
    }

  // calculate bounds as the min/max over all combinations of applying the
  // operator to lower/upper bound of left and right input
  private def minMaxAcrossAllCombinations(e: BinaryExpression):
      (Expression, Expression, Expression) = {
    val (l,r) = (e.children(0), e.children(1))
    val (llb,lbg,lub) = apply(l)
    val (rlb,rbg,rub) = apply(r)

    (
      Least(Seq(e.withNewChildren(Seq(llb,rlb)),e.withNewChildren(Seq(llb,rub)),e.withNewChildren(Seq(lub,rlb)),e.withNewChildren(Seq(lub,rub)))),
      e,
      Greatest(Seq(e.withNewChildren(Seq(llb,rlb)),e.withNewChildren(Seq(llb,rub)),e.withNewChildren(Seq(lub,rlb)),e.withNewChildren(Seq(lub,rub))))
    )
  }

  private def isCertainTrue(e: (Expression,Expression,Expression)): Expression = {
    getLB(e)
  }

  private def isCertainFalse(e: (Expression,Expression,Expression)): Expression = {
    negate(getUB(e))
  }

  private def negate(e: Expression) =
    e match {
      case Not(n) => n
      case Literal(x, BooleanType) => Literal(!x.asInstanceOf[Boolean])
      case _ => Not(e)
    }

  private def liftPointwise(e:Expression) =
  {
      val caveatedChildren = e.children.map { apply(_) }
      (e.withNewChildren(caveatedChildren.map(getLB)),
        e.withNewChildren(caveatedChildren.map(getBG)),
        e.withNewChildren(caveatedChildren.map(getUB))
      )
  }

  private def getLB(e:(Expression,Expression,Expression)): Expression =
    e._1

  private def getBG(e:(Expression,Expression,Expression)): Expression =
    e._2

  private def getUB(e:(Expression,Expression,Expression)): Expression =
    e._3

}
