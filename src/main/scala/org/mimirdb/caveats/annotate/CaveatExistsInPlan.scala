package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{ JoinType, Cross }
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.AliasIdentifier

import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.caveats._
import org.mimirdb.caveats.enumerate.EnumeratePlanCaveats
import org.mimirdb.caveats.Constants._
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

  def annotationEncoding = CaveatExistsBooleanArrayEncoding

  def annotationType = CaveatExistsType

  lazy val annotateAggregate  = new CaveatExistsInExpression(pedantic = pedantic,
                                                             expectAggregate = true)
  lazy val annotateExpression = annotateAggregate.withoutExpectingAggregate

  /**
   * Return a logical plan identical to the input plan, but with an additional
   * attribute containing caveat annotations.
   *
   * By analogy, if, after evaluation a row annotation is false, calling
   * EnumerateCaveats should not return any row-level caveats when limited to
   * the slice including the specified row.
   */
  def apply(plan: LogicalPlan): LogicalPlan =
  {
    Project(
     plan.output :+Alias(
        CreateNamedStruct(Seq(
          Literal(ROW_FIELD), CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression(),
          Literal(ATTRIBUTE_FIELD), CreateNamedStruct(
            plan.output.flatMap { attribute => 
              Seq(
                Literal(attribute.name),
                CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attribute.name)
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
      buildPlan(
        plan = plan,
        schema = plan.output,
        row = Literal(false),
        attributes = plan.output.map { attr => attr.name -> Literal(false) }
      )

    // Each operator has its own interactions with caveats.  Force an explicit
    // matching rather than using Spark's tree recursion operators and default
    // to fail-stop operation to make sure that new operators or non-standard
    // behaviors get surfaced early.

    val ret: LogicalPlan = plan match {

      /*********************************************************/
      case _ if Caveats.planIsAnnotated(plan) => 
        buildPlan(
          plan,
          schema = plan.output.filter { !_.equals(ANNOTATION_ATTRIBUTE) }
        )

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
        val annotation =
          buildAnnotation(
            rewrittenChild,
            plan.output,
            attributes = projectList.map { e => e.name -> annotateExpression(e) }
          )
        Project(
          projectList = projectList ++ annotation,
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
        val rewrittenChild = annotate(child)
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
        val rewrittenChild = annotate(child)
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
          condition: Option[Expression],
          hint: JoinHint) =>
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
            (left.output :+ 
              Alias(
                CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression(),
                CaveatExistsBooleanAttributeEncoding.rowAnnotationName(LEFT_ANNOTATION_ATTRIBUTE)
              )()) ++
              left.output.map { attr =>
                Alias(
                  CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attr.name),
                  CaveatExistsBooleanAttributeEncoding.attributeAnnotationName(attr.name, LEFT_ANNOTATION_ATTRIBUTE)
                )()                
              },
            annotate(left)
          )
        val rewrittenRight =
          Project(
            (right.output  :+ 
              Alias(
                CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression(),
                CaveatExistsBooleanAttributeEncoding.rowAnnotationName(RIGHT_ANNOTATION_ATTRIBUTE)
              )()) ++
              right.output.map { attr =>
                Alias(
                  CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attr.name),
                  CaveatExistsBooleanAttributeEncoding.attributeAnnotationName(attr.name, RIGHT_ANNOTATION_ATTRIBUTE)
                )()                
              },
            annotate(right)
          )

        val annotation = buildAnnotation(
          plan,
          plan.output
        )
        val attributes =
          left.output.map  { (LEFT_ANNOTATION_ATTRIBUTE, _)  } ++
          right.output.map { (RIGHT_ANNOTATION_ATTRIBUTE, _) }

        val basePlan = 
          buildPlan(
            // Rows in the output are caveated if either input tuple is
            // caveated.  Caveated join conditions are handled shortly.
            row = foldOr(
              CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression(LEFT_ANNOTATION_ATTRIBUTE),
              CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression(RIGHT_ANNOTATION_ATTRIBUTE)
            ),

            // Attribute caveats pass through from the source attributes.
            attributes =
              attributes.map { case (annotation, attr) =>
                attr.name ->
                  CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attr.name, annotation)
              },

            schema = plan.output,
            plan = Join(
              rewrittenLeft,
              rewrittenRight,
              joinType,
              condition,
              hint
            )
          )

        condition match {
          case None => basePlan
          case Some(actualCondition) => 
            buildPlan(
              row = foldOr(
                CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression(),
                annotateExpression(actualCondition)
              ),
              schema = plan.output,
              plan = basePlan
            )
        }

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
              negate(CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression()),
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
            aggr.name ->  foldOr(
                            annotateAggregate(aggr),
                            aggregateBoolOr(
                              if(isAggregate(aggr)){
                                foldOr(
                                  groupMembershipDependsOnACaveat,
                                  CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression()
                                )
                              } else { groupMembershipDependsOnACaveat }
                            )
                          )
          }

        val annotation =
          buildAnnotation(
            plan = child,
            schema = plan.output,
            row = rowAnnotation,
            attributes = attrAnnotations
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
          val CONTAMINATED_GROUPS = ANNOTATION_ATTRIBUTE+"_EXIST_IN_GROUPBY"
          val join =
            Join(
              Aggregate(Seq(),
                Seq(Alias(aggregateBoolOr(groupMembershipDependsOnACaveat),
                          CONTAMINATED_GROUPS)()),
                annotatedChild
              ),
              ret,
              Cross,
              None,
              JoinHint.NONE
            )
          Project(
            plan.output ++ buildAnnotation(
              plan = join,
              schema = plan.output,
              attributes =
                plan.output.map { attr =>
                    attr.name ->
                      foldOr(
                        CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attr.name),
                        UnresolvedAttribute(CONTAMINATED_GROUPS)
                      )
                 }
            ),
            join
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
        extendPlan(
          GlobalLimit(limitExpr, annotate(child)),
          plan.output,
          row = foldOr(possibleSortCaveats.map { _.isNonemptyExpression }:_*)
        )
      }

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) =>
      {
        val possibleSortCaveats = EnumeratePlanCaveats(child)(sort = true)
        extendPlan(
          LocalLimit(limitExpr, annotate(child)),
          plan.output,
          row = foldOr(possibleSortCaveats.map { _.isNonemptyExpression }:_*)
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
        ???
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
        ???
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

  def extendPlan(
    plan: LogicalPlan,
    schema: Seq[Attribute],
    row: Expression = null,
    attributes: Seq[(String, Expression)] = Seq()
  ): LogicalPlan =
  {
    val annotation =
      extendAnnotation(
        plan = plan,
        schema = schema,
        row = row,
        attributes = attributes
      )
    Project(
      schema ++ annotation,
      plan
    )
  }

  def buildPlan(
    plan: LogicalPlan,
    schema: Seq[Attribute],
    row: Expression = null,
    attributes: Seq[(String, Expression)] = null
  ): LogicalPlan =
  {
    val annotation =
      buildAnnotation(
        plan = plan,
        schema = schema,
        row = row,
        attributes = attributes
      )
    Project(
      schema ++ annotation,
      plan
    )
  }

  def extendAnnotation(
    plan: LogicalPlan,
    schema: Seq[Attribute],
    row: Expression = null,
    attributes: Seq[(String, Expression)] = Seq()
  ): Seq[NamedExpression] =
  {
    var rowAnnotation =
      CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression()
    if(row != null){
      rowAnnotation = Or(rowAnnotation, row)
    }

    var attributeAnnotations =
      schema.map { attribute =>
        attribute.name -> CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attribute.name)
      } ++ attributes

    buildAnnotation(
      plan,
      schema = schema,
      row = rowAnnotation,
      attributes = attributeAnnotations
    )
  }

  def buildAnnotation(
    plan: LogicalPlan,
    schema: Seq[Attribute],
    row: Expression = null,
    attributes: Seq[(String, Expression)] = null
  ): Seq[NamedExpression] =
  {
    val realRowAnnotation: NamedExpression =
      Alias(
        Option(row)
          .getOrElse { CaveatExistsBooleanAttributeEncoding.rowAnnotationExpression() },
        CaveatExistsBooleanAttributeEncoding.rowAnnotationName()
      )()

    val realAttributeAnnotations: Seq[NamedExpression] =
      Option(attributes)
        .map { attributes =>

          // CreateNamedStruct takes parameters in groups of 2: name -> value
          attributes.map { case (a, b) => 
            Alias(b, CaveatExistsBooleanAttributeEncoding.attributeAnnotationName(a))() 
          }
        }
        .getOrElse { 
          schema.map { attr => 
            Alias(
              CaveatExistsBooleanAttributeEncoding.attributeAnnotationExpression(attr.name),
              CaveatExistsBooleanAttributeEncoding.attributeAnnotationName(attr.name)
            )()
          }
        }

    realRowAnnotation +: realAttributeAnnotations
  }
}
