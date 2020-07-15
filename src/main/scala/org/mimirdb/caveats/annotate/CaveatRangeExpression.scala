package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedExtractValue,
  UnresolvedAttribute
}
import org.apache.spark.unsafe.types.UTF8String

import org.mimirdb.caveats._
import org.mimirdb.caveats.annotate._
import org.mimirdb.caveats.boundedtypes._

import org.mimirdb.spark.expressionLogic._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.codegen.TrueLiteral
import spire.syntax.group

case class RangeBoundedExpr[T <: Expression](lb: T, bg: T, ub: T)
{
  def toTuple(): (T,T,T) = (lb,bg,ub)

  def toSeq(): Seq[T] = Seq(lb,bg,ub)

  def map[S <: Expression](f: T => S): RangeBoundedExpr[S] = RangeBoundedExpr(f(lb), f(bg), f(ub))

  def zip[B](s: Seq[B]): Seq[(T,B)] = this.toSeq().zip(s)

  def applyToPairs[O <: Expression](r: RangeBoundedExpr[O], f: (T,O) => Expression) =
    RangeBoundedExpr(f(lb,r.lb),f(bg,r.bg),f(ub,r.ub))

  def applyIndividuallyToBounds[S <: Expression](flb: RangeBoundedExpr[T] => S,
    fbg: RangeBoundedExpr[T] => S,
    fub: RangeBoundedExpr[T] => S)
      : RangeBoundedExpr[S] =
    RangeBoundedExpr(flb(this), fbg(this), fub(this))

  def applyIndividually[S <: Expression](flb: T => S,
    fbg: T => S,
    fub: T => S)
      : RangeBoundedExpr[S] =
    RangeBoundedExpr(flb(lb), fbg(bg), fub(ub))

  def overlaps(r: RangeBoundedExpr[Expression]): Expression =
    And(LessThanOrEqual(lb,r.ub),LessThanOrEqual(r.lb,ub))

  def bgEqualTo(r: RangeBoundedExpr[Expression]): Expression = EqualTo(bg,r.bg)

  def isCertain(): Expression = EqualTo(lb,ub)

  def certainlyEqualTo[S <: Expression](r: RangeBoundedExpr[S]): Expression =
    foldAnd(isCertain(),r.isCertain(),EqualTo(lb,r.lb))

  def rename(names: Seq[String]): RangeBoundedExpr[NamedExpression] =
    rename((names(0), names(1), names(2)))

  def rename(names: (String,String,String)): RangeBoundedExpr[NamedExpression] =
    RangeBoundedExpr(
      Alias(lb,names._1)(),
      Alias(bg,names._2)(),
      Alias(ub,names._3)()
    )
}

object RangeBoundedExpr
{

  def makeCertain[T <: Expression](e: T): RangeBoundedExpr[T] = RangeBoundedExpr[T](e,e,e)
  def fromBounds[T <: Expression](lb: T, ub: T): RangeBoundedExpr[Expression] = RangeBoundedExpr(lb,Literal(1),ub)
  def fromTuple[T <: Expression](t: (T,T,T)): RangeBoundedExpr[T] = RangeBoundedExpr(t._1,t._2,t._3)
  def fromSeq[T <: Expression](s: Seq[T]): RangeBoundedExpr[T] = {
    //TODO check length
    assert(s.length == 3)
    RangeBoundedExpr(s(0),s(1),s(2))
  }
  def isCertainTrue(e: RangeBoundedExpr[Expression]): Expression = {
    e.lb
  }
  def isCertainFalse(e: RangeBoundedExpr[Expression]): Expression = {
    negate(e.ub)
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
    def applynogrp[T <: Expression](expr: T,trace: Boolean = false) = apply(expr,None,trace)

  /**
   * Derive an expression to compute the annotation of the input expression
   *
   * @param   expr  The expression to calculate bounds for
   * @return        A triple of expressions that computes lower, best guess, and upper bounds of the value of the input expression
   **/
  def apply[T <: Expression](
    expr: T,
    groupByAttrPairs: Option[Seq[(RangeBoundedExpr[NamedExpression],RangeBoundedExpr[Expression])]] = None,
    trace: Boolean = false
  ): RangeBoundedExpr[Expression] =
  {
    def tlog(message: String) = { if (trace) { println(message) } }

    tlog(s"----------------------------------------\nEXPR: $expr\nGB: $groupByAttrPairs\ntrace: $trace")

    // Derive a triple of conditions that calculates that lower bound, original expression, and upper bound for the
    // value of the expression across all possible worlds.
    expr match {

      // Access value and bounds stored in Caveat
      case caveat: CaveatRange => RangeBoundedExpr(caveat.lb, caveat.value, caveat.ub)

      // the bounds of a Literal are itself
      case l: Literal => RangeBoundedExpr.makeCertain(l)

      // Attributes are caveatted if they are caveated in the input.
      case a: Attribute => RangeBoundedExpr(
        CaveatRangeEncoding.attrLBexpression(a.name),
        CaveatRangeEncoding.attrBGexpression(a.name),
        CaveatRangeEncoding.attrUBexpression(a.name)
      )

      // Not entirely sure how to go about handling subqueries yet (requires attribute level ranges like aggregation)
      case sq: SubqueryExpression => throw new AnnotationException("Can't handle subqueries yet", expr)

      // comparison operators
      case EqualTo(left,right) => {
        val l = apply(left,groupByAttrPairs,trace)
        val r = apply(right,groupByAttrPairs,trace)

        RangeBoundedExpr(
          And(EqualTo(l.lb,r.ub),EqualTo(l.ub,r.lb)),
          EqualTo(l.bg,r.bg),
          And(LessThanOrEqual(l.lb,r.ub),LessThanOrEqual(r.lb,l.ub))
        )
      }

      case op:LessThanOrEqual => liftOrderComparison(op,groupByAttrPairs,trace)
      case op:LessThan => liftOrderComparison(op,groupByAttrPairs,trace)
      case op:GreaterThanOrEqual => liftOrderComparison(op,groupByAttrPairs,trace)
      case op:GreaterThan => liftOrderComparison(op,groupByAttrPairs,trace)

      // Arithemic
      case Add(left,right) => liftPointwise(expr,groupByAttrPairs,trace)

      case Subtract(left,right) => {
        val l = apply(left,groupByAttrPairs,trace)
        val r = apply(right,groupByAttrPairs,trace)

        RangeBoundedExpr(Subtract(l.lb,r.ub),expr,Subtract(l.ub,r.lb))
      }

      case Multiply(left,right) => minMaxAcrossAllCombinations(Multiply(left,right))

      case Divide(left,right) =>  minMaxAcrossAllCombinations(Divide(left,right))

      // conditional operators
      case If(predicate, thenClause, elseClause) =>
        {
          val cav_predicate = apply(predicate,groupByAttrPairs,trace)
          val cav_then = apply(thenClause,groupByAttrPairs,trace)
          val cav_else = apply(elseClause,groupByAttrPairs,trace)
          val certainTrue = RangeBoundedExpr.isCertainTrue(cav_predicate)
          val certainFalse = RangeBoundedExpr.isCertainFalse(cav_predicate)

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
            If(cav_predicate.bg, cav_then.bg, cav_else.bg),
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

      /**
        * We have to compute the min / max across all outcomes for all clauses
        * The only exception is if for a prefix of clauses all predicates
        * evaluate to certainly false followed by one clause predicate which is
        * certainly true (or there is an else clause). We have to test for each
        * such prefix. We do this by nesting, e.g., if the first clause's
        * predicate is certainly false, then we use another level of CaseWhen to
        * test whether the next predicate is certainly false or true. This way we
        * avoid potentially an factor n (number of clauses) blow-up in the size
        * of predicates.
        */
      case cas: CaseWhen => whenCases(cas)

      /* logical operators */
      case Not(child) => apply(child,groupByAttrPairs,trace).applyIndividuallyToBounds(x => Not(x.ub), x => Not(x.bg), x => Not(x.lb))

      case And(lhs, rhs) => liftPointwise(expr,groupByAttrPairs,trace)

      case Or(lhs, rhs) => liftPointwise(expr,groupByAttrPairs,trace)

      //TODO what about distinct does this require extra treatment?
      //TODO are there any aggregation function for which the bounding expressions are not an aggregation function?
      /**
        * For most aggregation functions we just rewrite the children except for Avg which we have to rewrite into Sum / Count
        */
      case AggregateExpression(
        aggregateFunction,
        mode,
        isDistinct,
        filter,
        _) =>
        {
          def rewriteOneAgg(agg: AggregateFunction): RangeBoundedExpr[Expression] =
            apply(agg,groupByAttrPairs,trace).map(x =>
              AggregateExpression(
                x.asInstanceOf[AggregateFunction],
                mode,
                isDistinct,
                NamedExpression.newExprId
                )
            )

          aggregateFunction match {
            case Average(e) => {
                rewriteOneAgg(Sum(e)).applyToPairs(
                  rewriteOneAgg(Count(Literal(1))),
                  {
                    (x:Expression,y:Expression) =>
                    CaseWhen(Seq((EqualTo(y,Literal(0)),Literal(0.0))),Divide(x,Cast(y,DoubleType))): Expression
                  }
                )
            }
            case _ => {
              rewriteOneAgg(aggregateFunction)
            }
          }
        }

      // aggregation function
      case agg:AggregateFunction =>
      {
        val r = CaveatRangeEncoding.rowAnnotationExpressionTriple()
        val g = groupByAttrPairs
        val hasGroups = g match { case Some(h) => true case None => false }

        // checks whether tuple should be counted int the best guess world (if it belongs to a group in the BGW). For aggregation without groupby this is always the case.
        val bgEquals: Expression = g match {
          case Some(gb) => foldAnd(gb.map{ case (l,r) => EqualTo(l.bg,r.bg) }:_*)
          case None => Literal(true)
        }

        // check whether tuple certainly belongs to a group (its group-by values are certain and it certainly exists: row.lb > 0)
        val certainGrpMember = g match {
          case Some(gb) =>
          And(
            foldAnd(gb.map { case (l,r) => l.certainlyEqualTo(r) }:_*),
            GreaterThan(r.lb,Literal(0))
          )
          case None => GreaterThan(r.lb,Literal(0))
        }

        // handle count separately
        //TODO deal with count DISTINCT
        agg match {
          case Count(children) => {
            RangeBoundedExpr(
              Sum(CaseWhen(Seq((certainGrpMember,r.lb)),Least(Seq(Literal(0),r.lb)))),
              Sum(if (hasGroups) CaseWhen(Seq((bgEquals,r.bg)),Least(Seq(Literal(0),r.bg))) else r.bg),
              Sum(r.ub)
            )
          }
          // sum, min, or max
          case _ =>
          {
              // calculate semimodule results for the input and pass on neutral value of the aggregation function
              val aggbounds: (RangeBoundedExpr[Expression],Literal) = agg match
                {
                  case Sum(e) =>
                  {
                    val s:Sum = agg.asInstanceOf[Sum]
                    (
                      apply(e,groupByAttrPairs,trace).applyIndividuallyToBounds(
                        x => Multiply(x.lb,CaseWhen(Seq((LessThan(x.lb,Literal(0)),r.ub)),r.lb)),
                        x => Multiply(x.bg,r.bg),
                        x => Multiply(x.ub,CaseWhen(Seq((GreaterThan(x.ub,Literal(0)),r.ub)),r.lb)),
                      ),
                      Literal.default(s.dataType)
                    )
                  }
                  case Min(e) => {
                    val s:Min = agg.asInstanceOf[Min]
                    (
                      apply(e,groupByAttrPairs,trace),
                      Literal.create(null,s.dataType)
                    )
                  }
                  case Max(e) => {
                    val s:Max = agg.asInstanceOf[Max]
                    (
                      apply(e,groupByAttrPairs,trace),
                      Literal.create(null,s.dataType)
                    )
                  }
                  case _ => throw new Exception(s"Unsupported aggregation function $agg")
                }
                // if group membership is uncertain then take least/greatest of A * row and the neutral element of the aggregation function and apply aggregation function to the result. For best guess we should only aggregate over tuples that exist in the BGW
                aggbounds._1.applyIndividually(
                  lb => CaseWhen(Seq((certainGrpMember,lb)),Least(Seq(aggbounds._2,lb))),
                  bg => CaseWhen(Seq((bgEquals,bg)),aggbounds._2),
                  ub => CaseWhen(Seq((certainGrpMember,ub)),Greatest(Seq(aggbounds._2,ub)))
                ).map{ x =>
                  agg match {
                    case Sum(e) => Sum(x)
                    case Min(e) => Min(x)
                    case Max(e) => Max(x)
                  }
                }.map{ x =>
                  tlog(x.sql)
                  x
                }
                  .asInstanceOf[RangeBoundedExpr[Expression]]
          }
        }
      }

      // works for and / or  too
      case _ => liftPointwise(expr,groupByAttrPairs,trace)
    }
  }

  // unresolve attribute references in expressions
  def unresolveAttrsInExpr(e: NamedExpression): NamedExpression = e match {
    case AttributeReference(name,dt,nullable,metadata) => UnresolvedAttribute(name)
    case _ => e.withNewChildren(e.children.map(unresolveAttrsInExpr(_))).asInstanceOf[NamedExpression]
  }

  def unresolveAttrsInExpr(e: Expression): Expression = e match {
    case AttributeReference(name,dt,nullable,metadata) => UnresolvedAttribute(name)
    case _ => e.withNewChildren(e.children.map(unresolveAttrsInExpr(_)))
  }

  // lift to (lub,rlb) and (llb,rub)
  def liftOrderComparison(
    op: BinaryExpression,
    groupByAttrPairs: Option[Seq[(RangeBoundedExpr[NamedExpression],RangeBoundedExpr[Expression])]] = None,
    trace: Boolean = false
  ) : RangeBoundedExpr[Expression] = {
    val (left,right) = (op.children(0), op.children(1))
    val l = apply(left,groupByAttrPairs,trace)
    val r = apply(right,groupByAttrPairs,trace)

    RangeBoundedExpr(
      op.withNewChildren(Seq(l.ub,r.lb)),
      op.withNewChildren(Seq(l.bg,r.bg)),
      op.withNewChildren(Seq(l.lb,r.ub))
    )
  }

  //TODO should not duplicate names!
  def preserveName(expr: NamedExpression): RangeBoundedExpr[NamedExpression] =
  {
    val e = apply(expr,None,false)
    RangeBoundedExpr(renameExpr(e.lb, expr.name), renameExpr(e.bg, expr.name), renameExpr(e.ub, expr.name))
  }

  def renameExpr(e: Expression, name: String): NamedExpression =
    e match {
      case Alias(expr,n) => Alias(expr,name)()
      case _ => Alias(e,name)()
    }

  // map a triple of boooleans into a triple of Int (Row annotations)
  def booleanToRowAnnotation(cond: RangeBoundedExpr[Expression]): RangeBoundedExpr[Expression] =
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
        UnresolvedAttribute(Seq(name))
        if CaveatRangeEncoding.isAnnotationAttribute(name.toString())
        => {
          val result = UnresolvedAttribute(attrToAnnotAttr(name.toString))
          // println(s"the result $result")
          result
        }
      case x:Expression => {
        val result = x.withNewChildren(
          x.children.map ( c =>
            replaceAnnotationAttributeReferences(c, attrToAnnotAttr)
          )
        )
        // println(s"the result is $result")
        result
      }
    }
  }

  def boolToInt(bool: Expression): Expression = {
    If(EqualTo(bool, Literal(true)), Literal(1), Literal(0))
  }

  def neutralRowAnnotation(): RangeBoundedExpr[Expression] = RangeBoundedExpr.makeCertain(Literal(1))

  // recursive function to deal with CASE WHEN clauses. For CASE WHEN (p1 o1) (p2 o2) ... e,
  // we calculate the lower bound (symmetrically the upper bound) as
  // CASE WHEN (certaintrue(p1) LB(o1))
  //           (certainfalse(p1) (CASE WHEN (certaintrue(p2) LB(o2))
  //                              ...
  //           least(LB(o1), (CASE WHEN (certaintrue(p2) ...
  private def whenCases(when: CaseWhen,
    groupByAttrPairs: Option[Seq[(RangeBoundedExpr[NamedExpression],RangeBoundedExpr[Expression])]] = None,
    trace: Boolean = false
  ): RangeBoundedExpr[Expression] =
    when match {
      case CaseWhen(Seq(), None) => null
      // a single when then clause
      case CaseWhen(Seq((pred, outcome)), None) => {
        val predB = apply(pred,groupByAttrPairs,trace)
        val outcomeB = apply(outcome,groupByAttrPairs,trace)
        // if when is not resolved then we cannot determine result types and cannot select the right values
        assert(when.resolved)
        val resultDT = when.dataType
        assert(BoundedDataType.isBoundedType(resultDT))
        RangeBoundedExpr(
          CaseWhen(
            Seq(
              (RangeBoundedExpr.isCertainTrue(predB), predB.lb)
            ),
            Some(Literal(BoundedDataType.domainMin(resultDT)))
          ),
          CaseWhen(
            Seq((predB.bg, outcomeB.bg))
          ),
          CaseWhen(
            Seq(
              (RangeBoundedExpr.isCertainTrue(predB), predB.ub)
            ),
            Some(Literal(BoundedDataType.domainMax(resultDT)))
          )
        )
      }
      // an else clause only
      case CaseWhen(Seq(), Some(els)) => {
            val elsB = apply(els,groupByAttrPairs,trace)
            RangeBoundedExpr( elsB.lb, elsB.bg, elsB.ub )
          }
      case CaseWhen((pred, outcome) +: otherClauses, els) => {
        val otherBounds = whenCases(CaseWhen(otherClauses, els))
        val predB = apply(pred,groupByAttrPairs,trace)
        val outcomeB = apply(outcome,groupByAttrPairs,trace)

        RangeBoundedExpr(
          CaseWhen(
            Seq(
              (RangeBoundedExpr.isCertainTrue(predB), outcomeB.lb),
              (RangeBoundedExpr.isCertainFalse(predB), otherBounds.lb)
            ),
            Least(Seq(outcomeB.lb, otherBounds.lb))
          ),
          CaseWhen(Seq((predB.bg, outcomeB.bg)), otherBounds.bg),
          CaseWhen(
            Seq(
              (RangeBoundedExpr.isCertainTrue(predB), outcomeB.ub),
              (RangeBoundedExpr.isCertainFalse(predB), otherBounds.ub)
            ),
            Greatest(Seq(outcomeB.ub, otherBounds.ub))
          )
        )
      }
    }

  // calculate bounds as the min/max over all combinations of applying the
  // operator to lower/upper bound of left and right input
  private def minMaxAcrossAllCombinations(
    e: BinaryExpression,
    groupByAttrPairs: Option[Seq[(RangeBoundedExpr[NamedExpression],RangeBoundedExpr[Expression])]] = None,
    trace: Boolean = false
  ): RangeBoundedExpr[Expression] = {
    val (left,right) = (e.children(0), e.children(1))
    val l = apply(left,groupByAttrPairs,trace)
    val r = apply(right,groupByAttrPairs,trace)

    RangeBoundedExpr(
      Least(Seq(e.withNewChildren(Seq(l.lb,r.lb)),e.withNewChildren(Seq(l.lb,r.ub)),e.withNewChildren(Seq(l.ub,r.lb)),e.withNewChildren(Seq(l.ub,r.ub)))),
      e.withNewChildren(Seq(l.bg,r.bg)),
      Greatest(Seq(e.withNewChildren(Seq(l.lb,r.lb)),e.withNewChildren(Seq(l.lb,r.ub)),e.withNewChildren(Seq(l.ub,r.lb)),e.withNewChildren(Seq(l.ub,r.ub))))
    )
  }

  private def negate(e: Expression) =
    e match {
      case Not(n) => n
      case Literal(x, BooleanType) => Literal(!x.asInstanceOf[Boolean])
      case _ => Not(e)
    }

  def withFreshExprIDs(e: Expression, trace: Boolean = false): Expression =
  {
    def tlog(message: String) = { if (trace) { println(message) } }

    tlog(e.getClass.getName + "@" + System.identityHashCode(e) + ": " + e.toString() )
    e match {
      case AggregateExpression(
        aggregateFunction,
        mode,
        isDistinct,
        resultId
      ) => {
        val newId = NamedExpression.newExprId
        tlog(s"REPLACE $resultId with $newId")
        AggregateExpression(
          withFreshExprIDs(aggregateFunction).asInstanceOf[AggregateFunction],
          mode,
          isDistinct,
          newId)
      }
      case CaseWhen(branches,els) => CaseWhen(branches.map{ case (x,y) => (withFreshExprIDs(x), withFreshExprIDs(y)) }, els.map(withFreshExprIDs(_)))
      case _ => e.withNewChildren(e.children.map( x => withFreshExprIDs(x,trace)))
    }
  }

  def liftPointwise(
    e:Expression,
    groupByAttrPairs: Option[Seq[(RangeBoundedExpr[NamedExpression],RangeBoundedExpr[Expression])]] = None,
    trace: Boolean = false
  ): RangeBoundedExpr[Expression] =
  {
    val caveatedChildren = e.children.map { apply(_,groupByAttrPairs,trace) }
    RangeBoundedExpr(
      e.withNewChildren(caveatedChildren.map(x => x.lb)),
      e.withNewChildren(caveatedChildren.map(x => x.bg)),
      e.withNewChildren(caveatedChildren.map(x => x.ub))
    )
  }
}
