package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{ JoinType, Cross }
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.execution.datasources.LogicalRelation
import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.caveats._
import org.mimirdb.caveats.enumerate.EnumeratePlanCaveats
import org.mimirdb.caveats.Constants._
import org.mimirdb.lenses.CaveatedDeduplicate
import org.mimirdb.spark.expressionLogic.{
  foldOr,
  foldAnd,
  foldIf,
  attributesOfExpression,
  aggregateBoolOr,
  negate,
  isAggregate
}

class CaveatExistsInPlan(
  pedantic: Boolean = true,
  ignoreUnsupported: Boolean = false,
  trace: Boolean = false
)
  extends AnnotationInstrumentationStrategy
  with LazyLogging
{

  def outputEncoding = CaveatExistsBooleanStructEncoding
  def internalEncoding: IntermediateEncoding = CaveatExistsAttributeAnnotation

  def annotationType = CaveatExistsType

  lazy val annotateAggregate  = new CaveatExistsInExpression(pedantic = pedantic,
                                                             expectAggregate = true,
                                                             planCompiler = this)
  lazy val annotateExpression = annotateAggregate.withoutExpectingAggregate

  /**
   * Return a logical plan identical to the input plan, but with an additional
   * attribute containing caveat annotations.
   *
   * By analogy, if, after evaluation a row annotation is false, calling
   * EnumerateCaveats should not return any row-level caveats when limited to
   * the slice including the specified row.
   */
  def apply(plan: LogicalPlan, trace: Boolean = false): LogicalPlan =
  {
    Project(
     plan.output
        .filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) }
      :+Alias(
        CreateNamedStruct(Seq(
          Literal(ROW_FIELD), internalEncoding.annotationForRow,
          Literal(ATTRIBUTE_FIELD), CreateNamedStruct(
            plan.output
                .filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) }
                .flatMap { attribute =>
                  Seq(
                    Literal(attribute.name),
                    internalEncoding.annotationFor(attribute)
                  )
                }
          )
        )),
        ANNOTATION_ATTRIBUTE
      )(),
      annotate(plan)
    )
  }

  def annotate(plan: LogicalPlan): LogicalPlan =
  {
    def PASS_THROUGH_CAVEATS =
      plan.mapChildren { annotate(_) }

    def PLAN_IS_FREE_OF_CAVEATS =
      internalEncoding.annotate(
        oldPlan = plan,
        newPlan = plan,
        default = Literal(false)
      )

    // Each operator has its own interactions with caveats.  Force an explicit
    // matching rather than using Spark's tree recursion operators and default
    // to fail-stop operation to make sure that new operators or non-standard
    // behaviors get surfaced early.

    val ret: LogicalPlan = plan match {

      /*********************************************************/
      case _ if Caveats.planIsAnnotated(plan) =>
        recoverExistingAnnotations(plan)

      /*********************************************************/
      case _:ReturnAnswer =>
      {
        /*
          A node automatically inserted at the top of query plans to allow
          pattern-matching rules to insert top-only operators.
        */
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case _:Subquery =>
      {
        /*
          A node automatically inserted at the top of subquery plans to
          allow for subquery-specific optimizatiosn.
        */
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case Project(projectList: Seq[NamedExpression], child: LogicalPlan) =>
      {
        /*
          The extended relational projection operator, analagous to Scala's
          'map'.  Each element of projectList defines an attribute in the
          schema resulting from this operator, and is evaluated over attributes
          in the input.
        */

        val rewrittenChild = annotate(child)
        val annotation = internalEncoding.annotations(
                            oldPlan = plan,
                            newChild = rewrittenChild,
                            attributes = projectList.map { e => e.toAttribute -> annotateExpression(e) }
                         )
        Project(
          projectList = projectList.map(
            CaveatExistsInExpression.replaceHasCaveat(_)).asInstanceOf[Seq[NamedExpression]]
            ++ annotation,
          child = rewrittenChild
        )
      }

      /*********************************************************/
      case Generate(
          generator: Generator,
          unrequiredChildIndex: Seq[Int],
          outer: Boolean,
          qualifier: Option[String],
          generatorOutput: Seq[Attribute],
          child: LogicalPlan) =>
      {
        /*
          Generate is analogous to flatMap or unnest.  This is projection,
          but where each projection is allowed to return multiple rows.

          The additional attributes in [generatorOutput] are added by
          [generator], which is basically just a normal expression.  Since (as
          of yet) we do not support complex types, we're just going to propagate
          any annotation triggered by the generator down to the generated
          attributes.

          Note that when [unrequiredChildIndex] is empty, the schema emitted by
          a generator is/should be a strict superset of [child].
        */
        //TODO deal with HasCaveat in here necessary?
        val generatorAnnotation = annotateExpression(generator)
        val rewrittenChild = annotate(child)
        val rewrittenPlan =
          Generate(
            generator = generator,
            unrequiredChildIndex = Seq(), // <--- this is the main change
            outer = outer,
            qualifier = qualifier,
            generatorOutput = generatorOutput,
            child = rewrittenChild        // <--- this is the other change
          )

        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = rewrittenPlan,

          // Only modify the generated attributes
          replace = generatorOutput.map { _ -> generatorAnnotation },

          // If there's a caveat on the generator, the set of rows might change
          addToRow = Seq(generatorAnnotation)
        )
      }

      /*********************************************************/
      case Filter(condition: Expression, child: LogicalPlan) =>
      {
        /*
          Filter is a normal where clause.  Caveats on the condition expression
          get propagated into the row annotation. If the user checks for caveats
          with HasCaveat(expr), we have to replace this with the result of
          CaveatExistsInExpression(expr).
        */
        val conditionAnnotation = annotateExpression(condition)
        val conditionReplacedHasCaveat = CaveatExistsInExpression.replaceHasCaveat(condition)
        val rewrittenChild = annotate(child)
        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = Filter(conditionReplacedHasCaveat, rewrittenChild),
          addToRow = Seq(conditionAnnotation)
        )
      }

      /*********************************************************/
      case Intersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean) =>
      {
        /*
          Return every tuple that appears in *both* left and right.

          This depends on the UAADB paper.
        */
        ???
      }

      /*********************************************************/
      case Except(left: LogicalPlan, right: LogicalPlan, isAll: Boolean) =>
      {
        /*
          Return every tuple that appears in left, and *not* right.

          This depends on the UAADB paper.
        */
        ???
      }

      /*********************************************************/
      case Union(children: Seq[LogicalPlan]) =>
      {
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case Join(
          left: LogicalPlan,
          right: LogicalPlan,
          joinType: JoinType,
          condition: Option[Expression],
          hint: JoinHint) =>
      {
        /*
          Normal relational join.  The main gimmick here is that we need to
          do a bit of renaming.  We rename the LHS/RHS annotations to disjoint
          names, and then add another projection after the fact to merge the
          annotations back together
        */

        internalEncoding.join(
          oldPlan = plan,
          lhs = annotate(left),
          rhs = annotate(right),
          build = Join(
            _:LogicalPlan,
            _:LogicalPlan,
            joinType,
            condition,
            hint
          ),
          addToRow = condition.map { annotateExpression(_) }.toSeq
        )
      }

      /*********************************************************/
      case InsertIntoDir(
          isLocal: Boolean,
          storage: CatalogStorageFormat,
          provider: Option[String],
          child: LogicalPlan,
          overwrite: Boolean) =>
      {
        /*
          Not entirely sure about this one, but it looks like an operator that
          materializes the intermediate state of a query plan.  If that's all
          it is, we should be able to leave it with a pass-through.
        */
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case View(desc: CatalogTable, output: Seq[Attribute], child: LogicalPlan) =>
      {
        /*
          Views are a bit weird.  We need to change the schema, which breaks the
          link to the view.  It's perfectly fine to materialize the annotated
          view... in which case it'll trigger the planIsAnnotated case above.
          If we've gotten here, we have to drop the link to the view.
        */
        annotate(child)
      }

      /*********************************************************/
      case With(child: LogicalPlan, cteRelations: Seq[(String, SubqueryAlias)]) =>
      {
        /*
          Common Table Expressions.  Basically, you evaluate each of the
          [cteRelations] in order, and expose the result to subsequent ctes and
          the child, which produces the final result.

          This is going to require a little care, since we need to invalidate
          any schemas cached in operators.
        */
        ???
      }

      /*********************************************************/
      case WithWindowDefinition(windowDefinitions: Map[String, WindowSpecDefinition], child: LogicalPlan) =>
      {
        /*
          Prepare window definitions for use in the child plan.

          This information should be passed down into the child...  Not sure how
          that's going to work though.
        */
        ???
      }

      /*********************************************************/
      case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) =>
      {
        /*
          Establish an order on the tuples.

          A little surprisingly, we don't actually do anything here.  SORT can
          not affect the contents of a relation unless paired with a LIMIT
          clause, so we handle the resulting row annotation there.  Caveats on
          the sort order are, in turn, handled by EnumerateCaveats
        */
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case Range(
          start: Long,
          end: Long,
          step: Long,
          numSlices: Option[Int],
          output: Seq[Attribute],
          isStreaming: Boolean) =>
      {
        /*
          Generate a sequence of integers

          This should be a leaf operator.  No caveats should be coming from here
        */
        PLAN_IS_FREE_OF_CAVEATS
      }

      /*********************************************************/
      case Aggregate(
          groupingExpressions: Seq[Expression],
          aggregateExpressions: Seq[NamedExpression],
          child: LogicalPlan) =>
      {
        /*
          A classical aggregate.

          Something to note here is that groupingExpressions is only used to
          define the set of group-by attributes.  aggregateExpressions is the
          actual projection list, and may include fragments to be evaluated both
          pre- and post-aggregate.
         */

        val annotatedChild = annotate(child)
        val groupingAttributes =
          groupingExpressions.flatMap { attributesOfExpression(_) }.toSet

        val groupMembershipDependsOnACaveat =
          foldOr(groupingExpressions.map { annotateExpression(_) }:_*)

        val groupIsGuaranteedToHaveRows =
          aggregateBoolOr(
            foldAnd(
              negate(internalEncoding.annotationForRow),
              negate(groupMembershipDependsOnACaveat)
            )
          )

        val rowAnnotation = negate(groupIsGuaranteedToHaveRows)



        val attrAnnotations =
          aggregateExpressions.map { aggr =>
            // If group membership depends on a caveat, it becomes necessary to
            // annotate each and every aggregate value, since they could change
            // (hence the groupMembershipDependsOnACaveat here).
            //
            // Additionally,
            // TODO: refine this so that group-by attributes don't get annotated
            // TODO: move grouping caveats out to table-level?
            aggr.toAttribute ->
              foldOr(
                annotateAggregate(aggr),
                aggregateBoolOr(
                  if(isAggregate(aggr)){
                    foldOr(
                      groupMembershipDependsOnACaveat,
                      internalEncoding.annotationForRow
                    )
                  } else { groupMembershipDependsOnACaveat }
                )
              )
          }

        val annotation =
          internalEncoding.annotations(
            oldPlan = plan,
            newChild = child,
            replace = attrAnnotations,
            row = rowAnnotation
          )

        val ret =
          Aggregate(
            groupingExpressions,
            aggregateExpressions ++ annotation,
            annotatedChild
          )

        // If we're being pedantic, than a caveatted group-by could be
        // hypothetically placed into ANY group, contaminating all attribute
        // annotations.
        if(pedantic && !groupMembershipDependsOnACaveat.equals(Literal(false))){

          val groupContaminant = AttributeReference("__MIMIR_ALL_GROUPS_CONTAMINATED", BooleanType)()

          internalEncoding.annotate(
            oldPlan = plan,
            newPlan =
              Join(
                ret,
                Aggregate(Seq(), Seq(
                    Alias(
                      aggregateBoolOr(groupMembershipDependsOnACaveat),
                      groupContaminant.name
                    )(groupContaminant.exprId)
                  ),
                  annotatedChild
                ),
                Cross,
                None,
                JoinHint.NONE
              ),
            replace =
              aggregateExpressions
                  .map { _.toAttribute }
                  .map { attr => attr -> foldOr(internalEncoding.annotationFor(attr),
                                                groupContaminant) }
          )
        } else { ret }
      }

      /*********************************************************/
      case Window(
          windowExpressions: Seq[NamedExpression],
          partitionSpec: Seq[Expression],
          orderSpec: Seq[SortOrder],
          child: LogicalPlan) =>
      {
        ???
      }

      /*********************************************************/
      case Expand(projections: Seq[Seq[Expression]], output: Seq[Attribute], child: LogicalPlan) =>
      {
        ???
      }

      /*********************************************************/
      case GroupingSets(
          selectedGroupByExprs: Seq[Seq[Expression]],
          groupByExprs: Seq[Expression],
          child: LogicalPlan,
          aggregations: Seq[NamedExpression]) =>
      {
        ???
      }

      /*********************************************************/
      case Pivot(
          groupByExprsOpt: Option[Seq[NamedExpression]],
          pivotColumn: Expression,
          pivotValues: Seq[Expression],
          aggregates: Seq[Expression],
          child: LogicalPlan) =>
      {
        ???
      }

      /*********************************************************/
      case GlobalLimit(limitExpr: Expression, child: LogicalPlan) =>
      {
        val possibleSortCaveats = EnumeratePlanCaveats(child)(sort = true)

        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = GlobalLimit(limitExpr, annotate(child)),
          addToRow = possibleSortCaveats.map { _.isNonemptyExpression }
        )
      }

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) =>
      {
        val possibleSortCaveats = EnumeratePlanCaveats(child)(sort = true)
        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = LocalLimit(limitExpr, annotate(child)),
          addToRow = possibleSortCaveats.map { _.isNonemptyExpression }
        )
      }

      /*********************************************************/
      case SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan) =>
      {
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case Sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long,
          child: LogicalPlan) =>
      {
        PASS_THROUGH_CAVEATS
      }

      /*********************************************************/
      case Distinct(child: LogicalPlan) =>
      {
        /*
          Return distinct records (i.e., toSet)

          We can no longer treat this as a normal distinct, since the annotation
          is a dependent attribute.  Convert to a (trivial) aggregate and
          rewrite that.
        */
        annotate(Aggregate(child.output, child.output, child))
      }

      /*********************************************************/
      case Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan) =>
      {
        ???
      }

      /*********************************************************/
      case RepartitionByExpression(
          partitionExpressions: Seq[Expression],
          child: LogicalPlan,
          numPartitions: Int) =>
      {
        ???
      }

      /*********************************************************/
      case OneRowRelation() =>
      {
        PLAN_IS_FREE_OF_CAVEATS
      }

      /*********************************************************/
      case Deduplicate(keys: Seq[Attribute], child: LogicalPlan) =>
      {
        annotate(CaveatedDeduplicate(keys, child))
      }

      /*********************************************************/
      case leaf:LeafNode => {
        PLAN_IS_FREE_OF_CAVEATS
      }
    }
    if(trace){
      println(s"ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
    } else {
      logger.trace(s"ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
    }

    return ret
  }

  def recoverExistingAnnotations(plan: LogicalPlan): LogicalPlan =
  {
    plan match {
      // We need to special-case filter, since it passes through its source schema.  Consider the
      // following example:
      //
      //    val df = old.annotated
      //    df.filter( ??? ).annotated
      //
      // The resulting data frame will detect as annotated, because filter just re-uses the schema.
      // We need to special-case it.  Specifically, recover annotations from the child, and then
      // process the filter as normal, ignoring the annotation attribute.
      //
      // Note that this is safe, because an annotated filter always has a projection over it.
      case Filter(condition, child) =>
        {
          val conditionAnnotation = annotateExpression(condition)
          val rewrittenChild = recoverExistingAnnotations(child)
          val planWithoutAnnotation = Project(
              plan.output.filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) },
              plan
            )
          internalEncoding.annotate(
            oldPlan = planWithoutAnnotation,
            newPlan = Filter(condition, rewrittenChild),
            addToRow = Seq(conditionAnnotation)
          )
        }

      // Otherwise, manually unpack the projection.  This may get a little hairy... it might be
      // a good idea eventually to add a special case to check to see if this is the projection
      // wrapper created in apply() above
      case project:Project => {
        // if this is not the wrapping projection, then give up and manually unpack the fields
        val fields = plan.output.filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) }
        val annotations =
          internalEncoding.annotations(
            oldPlan = Project(fields, plan),
            newChild = plan,
            attributes =
              fields.map { field =>
                field -> outputEncoding.attributeAnnotationExpressions(field.name)(0)
              },
            row = outputEncoding.rowAnnotationExpression()
          )
        Project(fields ++ annotations, project)
      }

      // It's also possible that we have something other than a projection,
      // for example, if we're reading from a materialized view.  That's
      // fine... create a fake projection on top and try again
      case _ => recoverExistingAnnotations(Project(plan.output, plan))
    }
  }
}
