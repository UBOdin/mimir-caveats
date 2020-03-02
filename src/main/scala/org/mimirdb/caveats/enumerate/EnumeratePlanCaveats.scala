package org.mimirdb.caveats.enumerate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.mimirdb.caveats._
import org.mimirdb.spark.expressionLogic.{
  foldOr,
  foldAnd, 
  inline,
  attributesOfExpression,
  splitAnd,
  isAggregate
}

/**
 * A class for enumerating caveats applied to a specified plan.
 *
 * In principle, what we are looking for is the set of caveat messages that 
 * affect a given subset of the result relation.  A "theoretically correct" 
 * implementation this class would literally annotate every row, field, etc... 
 * with every distinct caveat message and return the result.  This is not 
 * practical, so instead we adopt an approximation.
 * 
 * It is easiest to understand how EnumeratePlanCaveats works by analogy to the
 * idea of *slicing* from classical programming languages research. In the 
 * context of databases, the goal would be to find the minimal subset of the 
 * query required to compute a subset (horizontal and vertical) of the result 
 * relation.  For example, consider the query:
 * 
 *   SELECT A + B AS M, C + D AS N WHERE E > 3 FROM R
 * 
 * To produce the `M` attribute of the result relation we need the slice of R
 * containing:
 *   - rows where R.E > 3
 *   - columns A and B
 * 
 * Broadly, we define a set of slicing criteria in terms of horizontal criteria:
 *   - Row:   Include caveats that affect any given row's presence in the 
 *            result.
 *   - Field: Include caveats that affect the value of an attribute on any given
 *            row in the result.
 *   - Order: Include caveats that affect the sort order of the result relation.
 * and Row and Field slices are also defined in terms of a vertical criteria: 
 * the set of rows for which we are interested in this slice.
 * 
 * EnumerateCaveats works by visiting each operator top down with a set of 
 * slicing criteria and computing
 *   - If the operator includes an ApplyCaveat expression locally, the subset of
 *     the slice defined by the input criteria on which ApplyCaveat would be 
 *     invoked
 *   - A minimal set of slicing criteria each the child operator required to 
 *     produce the slice defined by the input criteria.  
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
   * @param  attributes Include attribute-level caveats for the specified attrs
   * @param  sort       True to include caveats affecting the sort order
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
    attributes: Set[String] = Set(),
    sort: Boolean = false,
    constraint: Expression = Literal(true)
  ): Seq[CaveatSet] = 
    recurPlan(
      if(row){ Some(constraint) } else { None }, 
      attributes.map { _ -> constraint }.toMap, 
      sort, 
      plan
    )

  def recurPlan(
    row: Option[Expression],
    attributes: Map[String,Expression],
    sort: Boolean,
    plan: LogicalPlan
  ): Seq[CaveatSet] = 
  {
    def PASS_THROUGH_TO_CHILD(u: UnaryNode) = 
      recurPlan(row, attributes, sort, u.child)

    // println(s"ENUMERATING($row, $attributes, $sort)\n$plan")

    plan match {

      /*********************************************************/
      case x:ReturnAnswer => PASS_THROUGH_TO_CHILD(x)

      /*********************************************************/
      case x:Subquery => PASS_THROUGH_TO_CHILD(x)

      /*********************************************************/
      case Project(projectList: Seq[NamedExpression], child: LogicalPlan) => 
      {
        val relevantProjections = 
          projectList.filter { expr => attributes contains expr.name }
        val localCaveats = 
          relevantProjections.flatMap { projectExpression => 
            val fieldVSlice = attributes(projectExpression.name)
            EnumerateExpressionCaveats(child, projectExpression, fieldVSlice)
          }
        val allChildDependencies = 
          relevantProjections.flatMap { projectExpression => 
            ExpressionDependency.attributes(
              projectExpression, 
              inline(attributes(projectExpression.name), projectList)
            )
          }
        val childDependenciesByField = 
          allChildDependencies
            .groupBy { _._1 }
            .mapValues { deps => foldOr(deps.map { _._2 }:_*) }

        val childCaveats = recurPlan(
          row = row.map { inline(_, projectList) },
          attributes = childDependenciesByField,
          sort = sort,
          plan = child
        )

        return localCaveats ++ childCaveats
      }

      /*********************************************************/
      case Generate(
        generator: Generator,
        unrequiredChildIndex: Seq[Int],
        outer: Boolean,
        qualifier: Option[String],
        generatorOutput: Seq[Attribute],
        child: LogicalPlan
      ) => 
      {
        val generatedField = generatorOutput.map { _.name }.toSet
        val (sliceGeneratorFields, 
             sliceChildFields) = 
                attributes.partition { case (name, _) => generatedField(name) }

        val simulatedProjectionList = 
          generatorOutput.map { attribute =>
            Alias(generator, attribute.name)()
          } ++ child.output

        // If any vertical slice refers to a generated field, we have a 
        // problem since we don't know how to inline these through the 
        // generator.  We can probably handle this on a case-by-case basis 
        // eventually, but for now fall back to conservatively widening the 
        // slice.
        def inlineGeneratorFields(vSlice: Expression): Expression =
        {
          if((attributesOfExpression(vSlice).map { _.name } 
                & generatedField).isEmpty)
          {
            vSlice
          } else {
            Literal(true)
          }
        }

        // Caveats on the generator apply uniformly to any field slice for
        // a generator attribute.  Union together the vertical parts of 
        // these slices to find the slice on which we care about the 
        // generator caveats.
        val generatorVSlice = 
          inlineGeneratorFields(
            foldOr(sliceGeneratorFields.values.toSeq:_*)
          )

        val generatorIsIrrelevant = 
          generatorVSlice.equals(Literal(false))

        // If we're not interested in any generator attributes, we don't care about
        // caveats on the generator
        val generatorCaveats = 
          if(generatorIsIrrelevant){
            Seq[CaveatSet]()
          } else { 
            EnumerateExpressionCaveats(child, generator, generatorVSlice)
          }

        // Link any new dependencies from the generator in to the rest of the
        // field slicing criteria.
        val fieldDependenciesToPropagate =
          mergeVerticalSlices(
            ExpressionDependency.attributes(
              generator, 
              generatorVSlice
            ).toSeq ++ 
              sliceChildFields.toSeq
          )

        val childCaveats = recurPlan(
          row = row.map { inlineGeneratorFields(_) },
          attributes = fieldDependenciesToPropagate,
          sort = sort,
          plan = child
        )

        return generatorCaveats ++ childCaveats
      }

      /*********************************************************/
      case Filter(condition: Expression, child: LogicalPlan) => 
      {
        val localCaveats = 
          row.map { EnumerateExpressionCaveats(plan, condition, _) }
             .getOrElse { Seq[CaveatSet]() }

        val fieldDependenciesToPropagate =
          mergeVerticalSlices(
            row.map { ExpressionDependency.attributes(condition, _).toSeq }
               .getOrElse { Seq[(String,Expression)]() } ++
            attributes.toSeq
          )

        val childCaveats = recurPlan(
          row = row,
          attributes = fieldDependenciesToPropagate,
          sort = sort,
          plan = child
        )

        return localCaveats ++ childCaveats
      }

      /*********************************************************/
      case Union(children) =>
      {
        return children.flatMap { child =>
          recurPlan(
            row = row,
            attributes = attributes,
            sort = sort,
            plan = child
          )
        }
      }

      /*********************************************************/
      case Join(left, right, joinType, conditionMaybe) => 
      {

        val rowAndCondition = row.flatMap { r => conditionMaybe.map { (r, _) } }

        val localCaveats = 
          rowAndCondition.map { case (row, condition) =>
              EnumerateExpressionCaveats(plan, condition, row) 
            }.getOrElse { Seq[CaveatSet]() }

        val fieldDependenciesToPropagate =
          mergeVerticalSlices(
            rowAndCondition.map { case (row, condition) => 
              ExpressionDependency.attributes(condition, row).toSeq 
            }.getOrElse { Seq[(String,Expression)]() } ++
            attributes.toSeq
          )

        val isLeft = left.output.map { _.name }.toSet
        val isRight = right.output.map { _.name }.toSet

        val (attributesToPropagateLeft, attributesToPropagateRight) =
          fieldDependenciesToPropagate.partition { field => isLeft(field._1) }

        val rowPredicates = 
          row.map { splitAnd(_) }.getOrElse { Seq() }
             .map { pred => 
                pred -> 
                  attributesOfExpression(pred).map { _.name }
             }

        val itIsSafeToPropagateRow =
          rowPredicates.exists { case (_, attrs) =>
            (isLeft & attrs).isEmpty || (isRight & attrs).isEmpty
          }
        val (rowSliceLeft, rowSliceRight) = 
          rowPredicates.partition { case (pred, attrs) =>
                                    !(attrs & isLeft).isEmpty 
                                  }
        
        val (rowSliceLeftMaybe, rowSliceRightMaybe) = 
          if(row.equals(None)) { (None, None) } else {
            (
              Some(foldAnd(rowSliceLeft.map { _._1 }:_*)),
              Some(foldAnd(rowSliceRight.map { _._1 }:_*)),
            )
          }

        return recurPlan(
                  row = rowSliceLeftMaybe,
                  attributes = attributesToPropagateLeft,
                  sort = false, // Joins force a reorder
                  plan = left
               ) ++ recurPlan(
                  row = rowSliceRightMaybe,
                  attributes = attributesToPropagateRight,
                  sort = false, // Joins force a reorder
                  plan = left
               )
      }

      /*********************************************************/
      case x:InsertIntoDir => PASS_THROUGH_TO_CHILD(x)

      /*********************************************************/
      case View(_, _, child) => 
        recurPlan(row = row, attributes = attributes, sort = sort, plan = child)

      /*********************************************************/
      case Sort(order, _, child) =>
      {
        val localCaveats = 
          if(sort){
            order.flatMap { EnumerateExpressionCaveats(plan, _, Literal(true)) }
          } else { Seq() }

        val fieldDependenciesToPropagate =
          if(sort) {
            mergeVerticalSlices(
              order.flatMap { ExpressionDependency.attributes(_) }.toSeq ++
              attributes.toSeq
            )
          } else { attributes }

        val childCaveats = recurPlan(
          row = row,
          attributes = fieldDependenciesToPropagate,
          sort = false, // sort is resolved
          plan = child
        )

        return localCaveats ++ childCaveats
      }

      /*********************************************************/
      case Aggregate(
          groupingExpressions: Seq[Expression],
          aggregateExpressions: Seq[NamedExpression],
          child: LogicalPlan) => 
      {
        val projections =
          aggregateExpressions.map { agg => agg.name -> agg }
                              .toMap

        // This function inlines attributes in a vertical slice so that they're
        // valid on `child`.  The main reason that we can't just use 'inline' is
        // that the expression may involve an aggregate.  If so, we give up
        // and pessimistically widen the slice to cover everything.
        // 
        // TODO: One possible improvement here would be to embed a condition 
        // that figures out whether the output tuple participates in a group
        // that satisfies the constraint.  This is analogous to [[Generate]] 
        // above
        def safeInlineAggregate(expr: Expression): Expression =
        {
          val rawInline = inline(expr, projections)
          if(isAggregate(rawInline)){ Literal(true) }
          else { rawInline }
        }

        val localCaveats = 
          attributes.flatMap { case (attribute, vSlice) =>  
            val expression = projections(attribute)
            val outsideCaveats = 
              EnumerateExpressionCaveats(
                plan, expression, vSlice, 
                AggregateInteraction.OUTER_ONLY
              )
            val insideCaveats =
              EnumerateExpressionCaveats(
                child, expression, safeInlineAggregate(vSlice), 
                AggregateInteraction.OUTER_ONLY
              )
            insideCaveats ++ outsideCaveats
          }.toSeq

        val propagatedChildFields =
          mergeVerticalSlices(
            attributes.flatMap { case (attribute, vSlice) => 
              // Should be safe to extract attributes without inlining since: 
              //  1. If the attribute is inside an aggregate, it's clearly 
              //     available in the child.
              //  2. If the attribute is outside of an aggregate, it must be in
              //     the grouping expressions, or the entire expression is 
              //     invalid.
              // The vSlice still needs to be inlined through.
              ExpressionDependency.attributes(
                projections(attribute),
                safeInlineAggregate(vSlice)
              )
            }.toSeq
          )

        val childCaveats =
          recurPlan(
            row = row.map { safeInlineAggregate(_) },
            attributes = propagatedChildFields,
            sort = false, // Aggregation breaks sort order
            plan = child
          )

        return localCaveats ++ childCaveats
      }

      /*********************************************************/
      case GlobalLimit(limitExpr: Expression, child: LogicalPlan) => 
      {
        val localCaveats = 
          row.map { EnumerateExpressionCaveats(plan, limitExpr, _) }
             .getOrElse { Seq[CaveatSet]() }

        val fieldDependenciesToPropagate =
          mergeVerticalSlices(
            row.map { ExpressionDependency.attributes(limitExpr, _).toSeq }
               .getOrElse { Seq[(String,Expression)]() } ++
            attributes.toSeq
          )

        val childCaveats = recurPlan(
          row = row,
          attributes = fieldDependenciesToPropagate,
          sort = sort,
          plan = child
        )

        return localCaveats ++ childCaveats
      }

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) => 
      {
        val localCaveats = 
          row.map { EnumerateExpressionCaveats(plan, limitExpr, _) }
             .getOrElse { Seq[CaveatSet]() }

        val fieldDependenciesToPropagate =
          mergeVerticalSlices(
            row.map { ExpressionDependency.attributes(limitExpr, _).toSeq }
               .getOrElse { Seq[(String,Expression)]() } ++
            attributes.toSeq
          )

        val childCaveats = recurPlan(
          row = row,
          attributes = fieldDependenciesToPropagate,
          sort = sort,
          plan = child
        )

        return localCaveats ++ childCaveats
      }

      /*********************************************************/
      case x:SubqueryAlias => PASS_THROUGH_TO_CHILD(x)

      /*********************************************************/
      case Distinct(child) => recurPlan(
          row = row,
          attributes = attributes,
          sort = sort,
          plan = Aggregate(child.output, child.output, child)
        )

      /*********************************************************/
      case l:LeafNode => Seq()
    }


  }

  def mergeVerticalSlices(
    deps: Seq[(String, Expression)]
  ): Map[String, Expression] =
    deps.groupBy(_._1)
        .mapValues { deps => foldOr(deps.map { _._2 }.toSeq:_*) }
}