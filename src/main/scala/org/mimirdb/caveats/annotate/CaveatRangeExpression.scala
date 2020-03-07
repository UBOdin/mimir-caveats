package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedExtractValue,
  UnresolvedAttribute
}


import org.mimirdb.caveats._
import org.mimirdb.caveats.annotate._
import org.mimirdb.caveats.boundedtypes._

case class RangeBoundedExpr(lb: Expression, bg: Expression, ub: Expression)
{
  def toTuple(): (Expression,Expression,Expression) = (lb,bg,ub)
  def toSeq(): Seq[Expression] = Seq(lb,bg,ub)
  def map(f: Expression => Expression): RangeBoundedExpr = RangeBoundedExpr(f(lb), f(bg), f(ub))
  def applyIndividually(flb: Expression => Expression,
    fbg: Expression => Expression,
    fub: Expression => Expression)
      : RangeBoundedExpr =
    RangeBoundedExpr(flb(lb), fbg(bg), fub(ub))
}

object RangeBoundedExpr
{

  def makeCertain(e: Expression): RangeBoundedExpr = RangeBoundedExpr(e,e,e)
  def fromBounds(lb: Expression, ub: Expression): RangeBoundedExpr = RangeBoundedExpr(lb,Literal(1),ub)
  def fromTuple(t: (Expression,Expression,Expression)): RangeBoundedExpr = RangeBoundedExpr(t._1,t._2,t._3)
  def fromSeq(s: Seq[Expression]): RangeBoundedExpr = {
    //TODO check length
    assert(s.length == 3)
    RangeBoundedExpr(s(0),s(1),s(2))
  }
}

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
  def apply(expr: Expression): RangeBoundedExpr =
  {
    // Derive a set of conditions under which  the input expression will be
    // caveatted.  By default, this occurs whenever a caveat is present or when
    // an input attribute has a caveat.
    expr match {

      // Access value and bounds stored in Caveat
      case caveat: CaveatRange => RangeBoundedExpr(caveat.lb, caveat.value, caveat.ub)

      // the bounds of a Literal are itself
      case l: Literal => RangeBoundedExpr.makeCertain(l)

      // Attributes are caveatted if they are caveated in the input.
      case a: Attribute => RangeBoundedExpr(CaveatRangeEncoding.lbExpression(CaveatRangeEncoding.attributeAnnotationExpressionFromAttr(a)),
        CaveatRangeEncoding.bgAttrExpression(a),
        CaveatRangeEncoding.ubExpression(CaveatRangeEncoding.attributeAnnotationExpressionFromAttr(a))
      )

      // Not entirely sure how to go about handling subqueries yet (requires attribute level ranges like aggregation)
      case sq: SubqueryExpression => throw new AnnotationException("Can't handle subqueries yet", expr)

      // comparison operators
      case EqualTo(left,right) => {
        val l = apply(left)
        val r = apply(right)

        RangeBoundedExpr(
          And(EqualTo(l.lb,r.ub),EqualTo(l.ub,r.lb)),
          expr,
          And(LessThanOrEqual(l.lb,r.ub),LessThanOrEqual(r.lb,l.ub))
        )
      }

      case op:LessThanOrEqual => liftOrderComparison(op)
      case op:LessThan => liftOrderComparison(op)
      case op:GreaterThanOrEqual => liftOrderComparison(op)
      case op:GreaterThan => liftOrderComparison(op)

      // Arithemic
      case Add(left,right) => liftPointwise(expr)

      case Subtract(left,right) => {
        val l = apply(left)
        val r = apply(right)

        RangeBoundedExpr(Subtract(l.lb,r.ub),expr,Subtract(l.ub,r.lb))
      }

      case Multiply(left,right) => minMaxAcrossAllCombinations(Multiply(left,right))

      case Divide(left,right) =>  minMaxAcrossAllCombinations(Divide(left,right))

      // conditional operators
      case If(predicate, thenClause, elseClause) =>
        {
          val cav_predicate = apply(predicate)
          val cav_then = apply(thenClause)
          val cav_else = apply(elseClause)
          val certainTrue = isCertainTrue(cav_predicate)
          val certainFalse = isCertainFalse(cav_predicate)

          //TODO for certain conditions we can omit the third clause. Add inference for detecting whether an expression's value will be certain
          RangeBoundedExpr(
            // If certainly true or false then only the then or else clause matters
            // otherwise we have to take the minimum or maximum over then and else
            CaseWhen(
              Seq(
                // check for certainly true, if yes then return then lower bound
                (certainTrue, cav_then.lb),
                // check for certainly false, if yes then return else lower bound
                (certainFalse, cav_else.lb)
              ),
              // if predicate result is uncertain the take the minimum of then and else lower bounds
              Least(Seq(cav_then.lb, cav_else.lb))
            ),
            expr,
            // symmetric to lower bound
            CaseWhen(
              Seq(
                // check for certainly true, if yes then return then upper bound
                (certainTrue, cav_then.ub),
                // check for certainly false, if yes then return else upper bound
                (certainFalse, cav_else.ub)
              ),
              // if predicate result is uncertain the take the maximum of then and else upper bounds
               Greatest(Seq(cav_then.ub, cav_else.ub))
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
      case cas: CaseWhen => whenCases(cas)

      //   val casesB = whenCases(branches :: els)
      //   (
      //     casesB._1,
      //     CaseWhen(branches, otherwise),
      //     casesB._2
      //   )
      // }

      /* logical operators */
      case Not(child) => apply(child).map(x => Not(x))

      case And(lhs, rhs) => liftPointwise(expr)

      case Or(lhs, rhs) => liftPointwise(expr)

      // works for and / or / too
      case _ => liftPointwise(expr)
    }
  }

  // lift to (lub,rlb) and (llb,rub)
  def liftOrderComparison(op: BinaryExpression) : RangeBoundedExpr = {
    val (left,right) = (op.children(0), op.children(1))
    val l = apply(left)
    val r = apply(right)

    RangeBoundedExpr(
      op.withNewChildren(Seq(l.ub,r.lb)),
      op,
      op.withNewChildren(Seq(l.lb,r.ub))
    )
  }

  //TODO should not duplicate names!
  def preserveName(expr: NamedExpression): RangeBoundedExpr =
  {
    val e = apply(expr)
    RangeBoundedExpr(Alias(e.lb, expr.name)(), Alias(e.bg, expr.name)(), Alias(e.ub, expr.name)())
  }

  // map a triple of boooleans into a triple of Int (Row annotations)
  def booleanToRowAnnotation(cond: RangeBoundedExpr): RangeBoundedExpr =
    cond.map(boolToInt)

  // replace references to attribute bounds annotations based on a mapping from attribute to annotation attribute
  // This is used for binary operators (join) where the Expression we are rewritting may refer to multiple children
  def replaceAnnotationAttributeReferences(
    e: Expression,
    attrToAnnotAttr: Map[String,String])
      : Expression =
  {
    e match {
      case
          UnresolvedExtractValue(
            UnresolvedExtractValue(
              UnresolvedAttribute(Seq(Constants.ANNOTATION_ATTRIBUTE)),
              Literal(Constants.ATTRIBUTE_FIELD,StringType),
            ),
            Literal(attr:String,StringType)
          )
          => {
            UnresolvedExtractValue(
              UnresolvedExtractValue(
                UnresolvedAttribute(attrToAnnotAttr(attr)),
                Literal(Constants.ATTRIBUTE_FIELD),
              ),
              Literal(attr,StringType)
            )
          }
      case x:Expression => {
        x.withNewChildren(
          x.children.map ( c =>
          replaceAnnotationAttributeReferences(c, attrToAnnotAttr)
          )
        )
      }
    }
  }

  def boolToInt(bool: Expression): Expression = {
    If(EqualTo(bool, Literal(true)), Literal(1), Literal(0))
  }

  def neutralRowAnnotation(): RangeBoundedExpr = RangeBoundedExpr.makeCertain(Literal(1))

  // recursive function to deal with CASE WHEN clauses. For CASE WHEN (p1 o1) (p2 o2) ... e,
  // we calculate the lower bound (symmetrically the upper bound) as
  // CASE WHEN (certaintrue(p1) LB(o1))
  //           (certainfalse(p1) (CASE WHEN (certaintrue(p2) LB(o2))
  //                              ...
  //           least(LB(o1), (CASE WHEN (certaintrue(p2) ...
  private def whenCases(when: CaseWhen): RangeBoundedExpr =
    when match {
      case CaseWhen(Seq(), None) => null
      // a single when then clause
      case CaseWhen(Seq((pred, outcome)), None) => {
        val predB = apply(pred)
        val outcomeB = apply(outcome)
        // if when is not resolved then we cannot determine result types and cannot select the right values
        assert(when.resolved)
        val resultDT = when.dataType
        assert(BoundedDataType.isBoundedType(resultDT))
        RangeBoundedExpr(
          CaseWhen(
            Seq(
              (isCertainTrue(predB), predB.lb)
            ),
            Some(Literal(BoundedDataType.domainMin(resultDT)))
          ),
          when,
          CaseWhen(
            Seq(
              (isCertainTrue(predB), predB.ub)
            ),
            Some(Literal(BoundedDataType.domainMax(resultDT)))
          )
        )
      }
      // an else clause only
      case CaseWhen(Seq(), Some(els)) => {
            val elsB = apply(els)
            RangeBoundedExpr( elsB.lb, els, elsB.ub )
          }
      case CaseWhen((pred, outcome) +: otherClauses, els) => {
        val otherBounds = whenCases(CaseWhen(otherClauses, els))
        val predB = apply(pred)
        val outcomeB = apply(outcome)

        RangeBoundedExpr(
          CaseWhen(
            Seq(
              (isCertainTrue(predB), outcomeB.lb),
              (isCertainFalse(predB), otherBounds.lb)
            ),
            Least(Seq(outcomeB.lb, otherBounds.lb))
          ),
          when,
          CaseWhen(
            Seq(
              (isCertainTrue(predB), outcomeB.ub),
              (isCertainFalse(predB), otherBounds.ub)
            ),
            Greatest(Seq(outcomeB.ub, otherBounds.ub))
          )
        )
      }
    }

  // calculate bounds as the min/max over all combinations of applying the
  // operator to lower/upper bound of left and right input
  private def minMaxAcrossAllCombinations(e: BinaryExpression): RangeBoundedExpr = {
    val (left,right) = (e.children(0), e.children(1))
    val l = apply(left)
    val r = apply(right)

    RangeBoundedExpr(
      Least(Seq(e.withNewChildren(Seq(l.lb,r.lb)),e.withNewChildren(Seq(l.lb,r.ub)),e.withNewChildren(Seq(l.ub,r.lb)),e.withNewChildren(Seq(l.ub,r.ub)))),
      e,
      Greatest(Seq(e.withNewChildren(Seq(l.lb,r.lb)),e.withNewChildren(Seq(l.lb,r.ub)),e.withNewChildren(Seq(l.ub,r.lb)),e.withNewChildren(Seq(l.ub,r.ub))))
    )
  }

  private def isCertainTrue(e: RangeBoundedExpr): Expression = {
    e.lb
  }

  private def isCertainFalse(e: RangeBoundedExpr): Expression = {
    negate(e.ub)
  }

  private def negate(e: Expression) =
    e match {
      case Not(n) => n
      case Literal(x, BooleanType) => Literal(!x.asInstanceOf[Boolean])
      case _ => Not(e)
    }

  // def applyPointwiseToTuple3[In,Out](
  //   f: In => Out,
  //   input: (In, In, In))
  //     : (Out, Out, Out) =
  // {
  //   (
  //     f(input._1),
  //     f(input._2),
  //     f(input._3)
  //   )
  // }

  def liftPointwise(e:Expression): RangeBoundedExpr =
  {
      val caveatedChildren = e.children.map { apply(_) }
      RangeBoundedExpr(e.withNewChildren(caveatedChildren.map(x => x.lb)),
        e.withNewChildren(caveatedChildren.map(x => x.bg)),
        e.withNewChildren(caveatedChildren.map(x => x.ub))
      )
  }
}
