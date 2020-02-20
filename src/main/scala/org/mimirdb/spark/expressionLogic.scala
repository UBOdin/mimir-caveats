package org.mimirdb.spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

object expressionLogic
{
  def attributesOfExpression(e: Expression):Set[Attribute] = 
  {
    e.collect[Attribute] { case a: Attribute => a }.toSet
  }

  def negate(e: Expression) = 
    e match {
      case Not(n) => n
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
    GreaterThan(
      AggregateExpression(
        Sum(foldIf(e){ Literal(1) }{ Literal(0) }),
        Complete,
        false,
        NamedExpression.newExprId
      ),
      Literal(0)
    )

  def inline(e: Expression, projectionList: Seq[NamedExpression]): Expression =
    inline(e, projectionList.map { expr => expr.name -> expr }.toMap)

  def inline(e: Expression, replacements: Map[String,Expression]): Expression =
  {
    e.transform { 
      case a:Attribute => replacements(a.name)
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