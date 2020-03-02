package org.mimirdb.caveats.annotate

// import org.apache.spark.sql.catalyst.expressions._
// import org.apache.spark.sql.catalyst.expressions.aggregate._
// import org.apache.spark.sql.catalyst.analysis._
// import org.apache.spark.sql.catalyst.plans.logical._
// import org.apache.spark.sql.catalyst.plans.JoinType
// import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
// import org.apache.spark.sql.catalyst.AliasIdentifier

import com.typesafe.scalalogging.LazyLogging

// import org.mimirdb.spark.expressionLogic.{
//   foldOr, 
//   foldIf,
//   attributesOfExpression,
//   aggregateBoolOr
// }

class AnnotateWithRowIds(
  rowIdColumn: String = "__MIMIR_ROWID"
)
  extends LazyLogging
{
  /**
   * Return a plan with an additional column containing a unique identifier
   * for each row in the input.  The column will be a Struct with a variable
   * number of fields.
   */
  def apply(plan: LogicalPlan): LogicalPlan =
  {
    val ret: LogicalPlan = plan match {

      /*********************************************************/
      case _ if planIsAnnotated(plan) => plan

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

        val rewrittenChild = apply(child)
        val annotation = 
          buildAnnotation(
            rewrittenChild, 
            attributes = projectList.map { e => e.name -> annotateExpression(e) }
          )
        Project(
          projectList = projectList :+ annotation,
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

        val generatorAnnotation = annotateExpression(generator)
        val rewrittenChild = apply(child)
        extendPlan(
          // If there's a caveat on the generator, the number of rows might
          // change.  
          row = generatorAnnotation,

          // If there's a caveat on the generator, it propagates to all 
          // generated attributes
          attributes = generatorOutput.map { attribute => 
            attribute.name -> generatorAnnotation 
          },

          // Reconstruct the generator operator to 
          // For safety, rebuild the generator to restore all output attributes 
          // until the next optimization pass.
          schema = plan.output,
          plan = Generate(
            generator = generator,
            unrequiredChildIndex = Seq(), // <--- this is the main change
            outer = outer,
            qualifier = qualifier,
            generatorOutput = generatorOutput,
            child = rewrittenChild        // <--- this is the other change
          )
        )
      }

      /*********************************************************/
      case Filter(condition: Expression, child: LogicalPlan) => 
      {
        /*
          Filter is a normal where clause.  Caveats on the condition expression
          get propagated into the row annotation.
        */
        val conditionAnnotation = annotateExpression(condition)
        val rewrittenChild = apply(child)
        extendPlan(
          plan = Filter(condition, rewrittenChild),
          schema = plan.output,
          row = conditionAnnotation
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
          condition: Option[Expression]) => 
      {
        /* 
          Normal relational join.  The main gimmick here is that we need to
          do a bit of renaming.  We rename the LHS/RHS annotations to disjoint
          names, and then add another projection after the fact to merge the
          annotations back together
        */

        val LEFT_ANNOTATION_ATTRIBUTE = ANNOTATION_ATTRIBUTE+"_LEFT"
        val RIGHT_ANNOTATION_ATTRIBUTE = ANNOTATION_ATTRIBUTE+"_RIGHT"
        val rewrittenLeft = 
          Project(
            left.output :+ Alias(
                UnresolvedAttribute(ANNOTATION_ATTRIBUTE), 
                LEFT_ANNOTATION_ATTRIBUTE
              )(),
            apply(left)
          )
        val rewrittenRight = 
          Project(
            right.output :+ Alias(
                UnresolvedAttribute(ANNOTATION_ATTRIBUTE), 
                RIGHT_ANNOTATION_ATTRIBUTE
              )(),
            apply(right)
          )

        val conditionAnnotation = condition.map { annotateExpression(_) }
        val annotation = buildAnnotation(
          plan,
        )
        val attributes = 
          left.output.map  { (LEFT_ANNOTATION_ATTRIBUTE, _)  } ++ 
          right.output.map { (RIGHT_ANNOTATION_ATTRIBUTE, _) }

        buildPlan(

          // Rows in the output are caveated if either input tuple is
          // caveated, or if there is a caveated join condition 
          row = foldOr(
            (Seq(
              Caveats.rowAnnotationExpression(LEFT_ANNOTATION_ATTRIBUTE),
              Caveats.rowAnnotationExpression(RIGHT_ANNOTATION_ATTRIBUTE),
            ) ++ conditionAnnotation):_*
          ),

          // Attribute caveats pass through from the source attributes.
          attributes = 
            attributes.map { case (annotation, attr) =>
              attr.name -> 
                Caveats.attributeAnnotationExpression(attr.name, annotation)
            },

          schema = plan.output,
          plan = Join(
            rewrittenLeft,
            rewrittenRight,
            joinType,
            condition
          )
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
        apply(child)
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

        val annotatedChild = apply(child)
        val groupingAttributes =
          groupingExpressions.flatMap { attributesOfExpression(_) }.toSet
        val aGroupingAttributeIsCaveated = 
          if(!groupingAttributes.isEmpty){
            foldOr((
              EnumeratePlanCaveats(child)(
                fields = groupingAttributes.map { _.name }
              ).map { caveatSet => Not(caveatSet.isEmptyExpression) }
            ):_*)
          } else { Literal(false) }
        val groupMembershipDependsOnACaveat =
          foldOr(groupingExpressions.map { annotateAggregate(_) }:_*)

        val rowAnnotation = groupMembershipDependsOnACaveat


        val attrAnnotations = 
          aggregateExpressions.map { aggr => 
            // If group membership depends on a caveat, it becomes necessary to
            // annotate each and every aggregate value, since they could change
            // (hence the groupMembershipDependsOnACaveat here).
            //
            // Additionally, 
            // TODO: refine this so that group-by attributes don't get annotated
            // TODO: move grouping caveats out to table-level?
            aggr.name -> foldOr(
                           annotateAggregate(aggr), 
                           groupMembershipDependsOnACaveat
                         )
          }

        val annotation = 
          buildAnnotation(
            plan = child,
            row = rowAnnotation,
            attributes = attrAnnotations
          )

        Aggregate(
          groupingExpressions,
          aggregateExpressions :+ annotation,
          apply(annotatedChild)
        )
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
        ???
      }

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) => ???

      /*********************************************************/
      case SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan) => ???

      /*********************************************************/
      case Sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long,
          child: LogicalPlan) => ???

      /*********************************************************/
      case Distinct(child: LogicalPlan) => ???

      /*********************************************************/
      case Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan) => ???

      /*********************************************************/
      case RepartitionByExpression(
          partitionExpressions: Seq[Expression],
          child: LogicalPlan,
          numPartitions: Int) => ???

      /*********************************************************/
      case OneRowRelation() => ???

      /*********************************************************/
      case Deduplicate(keys: Seq[Attribute], child: LogicalPlan) => ???

      /*********************************************************/
      case leaf:LeafNode => ???
    }
    logger.trace(s"ROWID ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
      
    return ret
  }

  def planIsAnnotated(plan: LogicalPlan): Boolean =
      plan.output.map { _.name }.exists { _.equals(rowIdColumn) }

}