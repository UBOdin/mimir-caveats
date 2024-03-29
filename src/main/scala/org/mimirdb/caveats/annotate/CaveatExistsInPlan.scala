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
  type InternalDescription = CaveatExistsAttributeAnnotation.description
  def internalEncoding: IntermediateEncoding[InternalDescription] = CaveatExistsAttributeAnnotation

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
    val (annotatedPlan, description) = annotate(plan)

    Project(
     plan.output
        .filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) }
      :+Alias(
        CreateNamedStruct(Seq(
          Literal(ROW_FIELD), description.annotationForRow,
          Literal(ATTRIBUTE_FIELD), CreateNamedStruct(
            plan.output
                .filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) }
                .flatMap { attribute =>
                  Seq(
                    Literal(attribute.name),
                    description.annotationFor(attribute)
                  )
                }
          )
        )),
        ANNOTATION_ATTRIBUTE
      )(),
      annotatedPlan
    )
  }

  def annotate(plan: LogicalPlan): (LogicalPlan, InternalDescription) =
  {
    def PASS_THROUGH_CAVEATS: (LogicalPlan, InternalDescription) =
    {
      val (newChildren, descriptions) = 
        plan.children.map { annotate(_) }.unzip

      if(newChildren.size == 1){
        (plan.withNewChildren(newChildren), descriptions.head)
      } else {
        val (mergedExpressions, mergedDescription) = 
          internalEncoding.merge(descriptions)
        (
          Project(
            plan.output ++ mergedExpressions,
            plan.withNewChildren(newChildren)
          ),
          mergedDescription
        )
      }
    }

    def PLAN_IS_FREE_OF_CAVEATS: (LogicalPlan, InternalDescription) =
      internalEncoding.annotate(
        oldPlan = plan,
        newPlan = plan,
        newPlanDescription = null,
        default = Literal(false)
      )
    // assert(plan.analyzed, s"annotating non-analyzed plan: $plan")

    // Each operator has its own interactions with caveats.  Force an explicit
    // matching rather than using Spark's tree recursion operators and default
    // to fail-stop operation to make sure that new operators or non-standard
    // behaviors get surfaced early.

    val (ret, retDescription): (LogicalPlan, InternalDescription) = plan match {

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

        val (rewrittenChild, childDescription) = annotate(child)
        val (annotation, description) = 
          internalEncoding.annotations(
            oldPlan = plan,
            newPlanDescription = childDescription,
            attributes = projectList.map { e => e.toAttribute -> annotateExpression(e, childDescription) }
          )
        (
          Project(
            projectList = projectList.map(
              CaveatExistsInExpression.replaceHasCaveat(_, childDescription)).asInstanceOf[Seq[NamedExpression]]
              ++ annotation,
            child = rewrittenChild
          ),
          description
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
        val (rewrittenChild, childDescription) = annotate(child)
        val generatorAnnotation = annotateExpression(generator, childDescription)
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
          newPlanDescription = childDescription,

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
        val (rewrittenChild, childDescription) = annotate(child)
        val conditionAnnotation = annotateExpression(condition, childDescription)
        val conditionReplacedHasCaveat = 
          CaveatExistsInExpression.replaceHasCaveat(condition, childDescription)
        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = Filter(conditionReplacedHasCaveat, rewrittenChild),
          newPlanDescription = childDescription,
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
      case Union(children: Seq[LogicalPlan], byName, allowMissingCol) =>
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
        val (annotatedLeft, leftDescription) = annotate(left)
        val (annotatedRight, rightDescription) = annotate(right)

        val (mergedExpressions, mergedDescription) = 
          internalEncoding.merge(Seq(leftDescription, rightDescription))

        (
          Project(
            plan.output ++ mergedExpressions,
            Join(
              annotatedLeft,
              annotatedRight,
              joinType,
              condition,
              hint
            )
          ),
          mergedDescription
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
      case View(
            desc: CatalogTable, 
            isTempView: Boolean, 
            child: LogicalPlan
          ) =>
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

        val (annotatedChild, childDescription) = annotate(child)
        val groupingAttributes =
          groupingExpressions.flatMap { attributesOfExpression(_) }.toSet

        val groupMembershipDependsOnACaveat =
          foldOr(groupingExpressions.map { annotateExpression(_, childDescription) }:_*)

        val groupIsGuaranteedToHaveRows =
          aggregateBoolOr(
            foldAnd(
              negate(childDescription.annotationForRow),
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
            getResolvedAttribute(aggr) ->
              foldOr(
                annotateAggregate(aggr, childDescription),
                aggregateBoolOr(
                  if(isAggregate(aggr)){
                    foldOr(
                      groupMembershipDependsOnACaveat,
                      childDescription.annotationForRow
                    )
                  } else { groupMembershipDependsOnACaveat }
                )
              )
          }

        val (annotation, description) =
          internalEncoding.annotations(
            oldPlan = plan,
            newPlanDescription = childDescription,
            replace = attrAnnotations,
            attributes = attrAnnotations,
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

          val attrAnnotations = 
            aggregateExpressions
              .map { getResolvedAttribute(_) }
              .map { attr => attr -> foldOr(description.annotationFor(attr),
                                            groupContaminant) }

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
            newPlanDescription = description,
            replace = attrAnnotations,
            attributes = attrAnnotations
              
          )
        } else { (ret, description) }
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

        val (annotatedChild, childDescription) = annotate(child)

        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = GlobalLimit(limitExpr, annotatedChild),
          newPlanDescription = childDescription,
          addToRow = possibleSortCaveats.map { _.isNonemptyExpression }
        )
      }

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) =>
      {
        val possibleSortCaveats = EnumeratePlanCaveats(child)(sort = true)
        
        val (annotatedChild, childDescription) = annotate(child)
        
        internalEncoding.annotate(
          oldPlan = plan,
          newPlan = LocalLimit(limitExpr, annotatedChild),
          newPlanDescription = childDescription,
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
          numPartitions: Option[Int]
        ) =>
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

    return (ret, retDescription)
  }

  def recoverExistingAnnotations(plan: LogicalPlan): (LogicalPlan, InternalDescription) =
  {
    if(trace){
      println(s"Recover Existing Annotations: \n$plan")      
    } else {
      logger.trace(s"Recover Existing Annotations: \n$plan")      
    }
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
          val (rewrittenChild, description) = recoverExistingAnnotations(child)
          val conditionAnnotation = annotateExpression(condition, description)
          val planWithoutAnnotation = Project(
              plan.output.filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) },
              plan
            )
          internalEncoding.annotate(
            oldPlan = planWithoutAnnotation,
            newPlan = Filter(condition, rewrittenChild),
            newPlanDescription = description,
            addToRow = Seq(conditionAnnotation)
          )
        }

      // Otherwise, manually unpack the projection.  This may get a little hairy... it might be
      // a good idea eventually to add a special case to check to see if this is the projection
      // wrapper created in apply() above
      case project:Project => {
        // if this is not the wrapping projection, then give up and manually unpack the fields
        val fields = plan.output.filterNot { _.name.equals(ANNOTATION_ATTRIBUTE) }
        val (annotations, description) =
          internalEncoding.annotations(
            oldPlan = Project(fields, plan),
            newPlanDescription = null, // we're overriding the attributes and row, description shouldn't be needed
            attributes =
              fields.map { field =>
                field -> 
                  outputEncoding.attributeAnnotationExpressions(field.name)(0)
              },
            row = outputEncoding.rowAnnotationExpression()
          )
        (
          Project(fields ++ annotations, project),
          description
        )
      }

      // It's also possible that we have something other than a projection,
      // for example, if we're reading from a materialized view.  That's
      // fine... create a fake projection on top and try again
      case _ => recoverExistingAnnotations(Project(plan.output, plan))
    }
  }

  /**
   * Convert a (resolved) named expression to a *resolved* attribute, regardless 
   * of what Spark thinks you should get.
   * 
   * This function exists because calling _.toAttribute on an Alias 
   * expression will sometimes not return a resolved attribute, even though
   * there is an exprId
   */
  def getResolvedAttribute(base: NamedExpression) =
  {
    base match {
      case a:Alias => 
        AttributeReference(
          a.name,
          a.child.dataType,
          a.child.nullable,
          a.metadata,
        )(
          a.exprId,
          a.qualifier
        )
      case _ => base.toAttribute
    }
  }
}
