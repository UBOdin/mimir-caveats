package org.mimirdb.spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

object expressionLogic
{
  def attributesOfExpression(e: Expression):Set[Attribute] =
  {
    e match { 
      case a: Attribute => Set(a)
      case s: SubqueryExpression => 
      {
        val isPlanAttribute = s.plan.output.map { _.exprId }.toSet
        // print(s"PLAN ATTRIBUTES: $isPlanAttribute")
        (
          s.children
           .flatMap { attributesOfExpression(_) }
           .filter { a => !isPlanAttribute(a.exprId) }
           .toSet 
        )
      }
      case _ => e.children.flatMap { attributesOfExpression(_) }.toSet
    }
  }

  // /**
  //   * Construct a new expression tree by applying [[f]] to all nodes in the
  //   * expression tree rooted at [[e]].
  //   *  @param e input expression
  //   *  @param f function the constructs the new expression (
  //   */
  // def mutateExpr(e: Expression, f: Expression => Expression) = {

  // }

  def negate(e: Expression): Expression =
    e match {
      case Not(n) => n
      case Or(l, r) => And(negate(l), negate(r))
      case And(l, r) => Or(negate(l), negate(r))
      case Literal(x, BooleanType) => Literal(!x.asInstanceOf[Boolean])
      case _ => Not(e)
    }
  def foldOr(e:Expression*) = fold(e, true)
  def foldAnd(e:Expression*) = fold(e, false)
  def foldIf(i:Expression)(t: Expression)(e: Expression) =
    e match {
      case Literal(true, BooleanType) => t
      case Literal(false, BooleanType) => e
      case _ => If(i, t, e)
    }

  def aggregateBoolOr(e:Expression) =
    e match {
      case Literal(false, BooleanType) => Literal(false)
      case _ => //BoolOr("bool_or", e)
        GreaterThan(
          AggregateExpression(
            Sum(foldIf(e){ Literal(1) }{ Literal(0) }),
            Complete,
            false,
            None,
            NamedExpression.newExprId
          ),
          Literal(0)
        )
    }

  def wrapAgg(e: AggregateFunction, distinct: Boolean = false) = {
    AggregateExpression(
      e,
      Complete,
      distinct,
      None,
      NamedExpression.newExprId
    )
  }

  def simplify(e: Expression): Expression =
  {
    // println(s"SIMPLIFY: $e")
    if(e.references.isEmpty){ Literal(e.eval(), e.dataType) }
    else { 
      e.mapChildren { 
        case r:RuntimeReplaceable => simplify(r.child)
        case x => simplify(x) 
      } 
    }
  }

  def inline(e: Expression, projectionList: Seq[NamedExpression]): Expression =
    inline(e, projectionList.map { expr => expr.exprId -> expr }.toMap)

  def inline(e: Expression, replacements: Map[ExprId,Expression]): Expression =
  {
    e match {
      // Base case: An attribute that can be replaced
      case a:Attribute => replacements.getOrElse(a.exprId, a)

      // RuntimeReplaceable expressions are internally equivalent to/replacable
      // with their _.child field.  The outer shell may have other subexpressions,
      // but these are only ever used for display purposes.  Unfortunately, 
      // _.mapChildren only gets applied to the _.child field, so inlining ends
      // up creating expression trees that make no sense when printed.  In the
      // interest of making debugging easier, just flatten out the expression
      // here and now.  
      case r:RuntimeReplaceable => inline(r.child, replacements)

      // Recursive case: Replace all descendants.
      case _ => e.mapChildren { inline(_, replacements) }
    }
  }

  def isAggregate(e: Expression): Boolean =
  {
    e match {
      case _:AggregateExpression => true
      case _ => e.children.exists { isAggregate(_) }
    }
  }

  private def fold(
    conditions: Seq[Expression],
    disjunctive: Boolean = true
  ): Expression =
    conditions.filter { !_.equals(Literal(!disjunctive)) } match {
      // Non-caveatted node.
      case Seq() => Literal(!disjunctive)

      // One potential caveat.
      case Seq(condition) => condition

      // Always caveatted node
      case c if c.exists { _.equals(Literal(disjunctive)) } => Literal(disjunctive)

      // Multiple potential caveats
      case c =>
        val op = (if(disjunctive) { Or(_,_) } else { And(_,_) })
        c.tail.foldLeft(c.head) { op(_,_) }
    }

  def splitAnd(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitAnd(cond1) ++ splitAnd(cond2)
      case other => other :: Nil
    }
  }

  def splitOr(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitOr(cond1) ++ splitOr(cond2)
      case other => other :: Nil
    }
  }
}
