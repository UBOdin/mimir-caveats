package org.mimirdb.caveats.enumerate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.mimirdb.caveats._
import org.mimirdb.spark.expressionLogic.{
  foldOr
}

/**
 * A class for enumerating caveats applied to a specified plan.
 *
 * We break caveats down into five classes: 
 *  - Table:     Indicating that there are rows or attributes that could be 
 *               added to the table if assumptions change.  Table level caveats  
 *               can alsobe used to capture coarse-grained dependencies (i.e., 
 *               assumptions that do not have well-defined effects on the table)
 *  - Row:       Indicating that changing assumptions could exclude this row 
 *               from the relation.
 *  - Attribute: Indicating schema-level assumptions that could change the 
 *               structure of the attribute.
 *  - Field:     Indicating assumptions that could lead to this specific value 
 *               changing.
 *  - Sort:      Indicating assumptions that could lead to the sort order of the
 *               table's records changing.
 * 
 * In my ideal world, we would have a separate method for enumerating each type.
 * One for enumerating field caveats, one for sort caveats, one for row caveats,
 * etc...  Unfortunately this requires a TON of dupicated code that makes me
 * cringe.  I can't bring myself to create that sort of a maintenance nightmare,
 * so instead what we're going to do is to keep a [[SlicingState]].
 * 
 * The [[SlicingState]] class abstracts out 

 */


object EnumeratePlanCaveats
{
  /**
   * Enumerate the caveats affecting some slice of the logical plan.
   *
   * The first batch of parameters is simply the logical plan
   * @param  plan       The logical plan to enumerate
   *
   * The second batch of parameters identifies the target slice
   * @param  row        True to include row-level caveats
   * @param  fields     Include attribute-level caveats for the specified fields
   * @param  order      True to include caveats affecting the sort order
   * @param  table      True to include table-level caveats
   * @param  attributes True to include attribute-level caveats
   * @param  constraint Limit the result to caveats on rows satisfying the 
   *                    indicated (optional) condition
   * 
   * @return            A set of [[CaveatSet]]s enumerating the caveats.
   *
   * There may be a very large number of [[Caveat]]s affecting the target plan.  
   * Instead of returning each and every single one, EnumerateCaveats returns
   * placeholders caled [[CaveatSet]]s in a static analysis of the plan.  For
   * the full list of [[Caveat]]s, each [[CaveatSet]] may be further enumerated.
   */
  def apply(
    plan: LogicalPlan
  )(
    row: Boolean = false,
    fields: Set[String] = Set(),
    order: Boolean = false,
    constraint: Expression = Literal(true)
  ): Seq[CaveatSet] = 
    recurPlan(
      if(row){ Some(constraint) } else { None }, 
      fields.map { _ -> constraint }.toMap, 
      order, 
      plan
    )

  def recurPlan(
    row: Option[Expression],
    fields: Map[String,Expression],
    order: Boolean,
    plan: LogicalPlan
  ): Seq[CaveatSet] = 
  {
    def PASS_THROUGH_TO_CHILD(u: UnaryNode) = 
      recurPlan(row, fields, order, u.child)

    plan match {
      case x:ReturnAnswer => PASS_THROUGH_TO_CHILD(x)
      case x:Subquery => PASS_THROUGH_TO_CHILD(x)
      case Project(projectList: Seq[NamedExpression], child: LogicalPlan) => 
      {
        val relevantProjections = 
          projectList.filter { expr => fields contains expr.name }
        val relevantLocalCaveats = 
          relevantProjections.flatMap { projectExpression => 
            val fieldCondition = fields(projectExpression.name)
            EnumerateExpressionCaveats(child, projectExpression, fieldCondition)
          }
        val allChildDependencies = 
          relevantProjections.flatMap { projectExpression => 
            ExpressionDependency.attributes(projectExpression)
          }
        val childDependenciesByField = 
          allChildDependencies
            .groupBy { _._1 }
            .mapValues { deps => foldOr(deps.map { _._2 }:_*) }

        val childCaveats = recurPlan(
          row = row,
          fields = childDependenciesByField,
          order = order,
          plan = child
        )

        return relevantLocalCaveats ++ childCaveats
      }

      case l:LeafNode => Seq()
    }
  }
}