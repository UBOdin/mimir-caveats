package mimir.caveats

import org.apache.spark.sql.catalyst.expressions._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._

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
 *    [AnnotatePlan] (i.e., there is a [Caveats.ANNOTATION] column).
 **/
object AnnotateExpression
  extends LazyLogging
{
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

      case a: Attribute => Caveats.attributeAnnotationExpression(a)
      
      case sq: SubqueryExpression => throw new AnnotationException("Can't handle subqueries yet", expr)
      
      case If(predicate, thenClause, elseClause) => 

        // Two possible sources of taint: 'predicate' and the clauses.
        // Consider each case separately and then combine disjunctively.
        fold(Seq(

          // If 'predicate' is contaminated, it will contaminate the entire
          // expression.  
          apply(predicate), 

          // Otherwise, propagate the annotation from whichever branch ends
          // up being taken
          If(predicate, apply(thenClause), apply(elseClause))
        ))

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

      
      // TODO: Caveat, If, Case, CaseWhen, And, Or

      case _ => fold(expr.children.map { apply(_) })
    }
  }

  def preserveName(expr: NamedExpression): NamedExpression =
  {
    Alias(apply(expr), expr.name)()
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
}