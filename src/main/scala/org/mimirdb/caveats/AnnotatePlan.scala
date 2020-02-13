package org.mimirdb.caveats

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.AliasIdentifier

import com.typesafe.scalalogging.LazyLogging

object AnnotatePlan
  extends LazyLogging
{

  /** 
   * Return a logical plan identical to the input plan, but with an additional 
   * column containing caveat annotations. 
   */
  def apply(plan: LogicalPlan): LogicalPlan =
  {
    def PASS_THROUGH_CAVEATS = plan.mapChildren { apply(_) }

    // Each operator has its own interactions with caveats.  Force an explicit
    // matching rather than using Spark's tree recursion operators and default 
    // to fail-stop operation to make sure that new operators or non-standard
    // behaviors get surfaced early.

    val ret = plan match {
      case _:ReturnAnswer => 
      {
        /* 
          A node automatically inserted at the top of query plans to allow
          pattern-matching rules to insert top-only operators.  
        */
        PASS_THROUGH_CAVEATS
      }
      case _:Subquery =>
      {
        /*
          A node automatically inserted at the top of subquery plans to 
          allow for subquery-specific optimizatiosn.  
        */
        PASS_THROUGH_CAVEATS
      }
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
            colAnnotations = 
              projectList.map { e => e.name -> AnnotateExpression(e) }
          )
        Project(
          projectList :+ annotation,
          rewrittenChild
        )
      }
      case Generate(
          generator: Generator,
          unrequiredChildIndex: Seq[Int],
          outer: Boolean,
          qualifier: Option[String],
          generatorOutput: Seq[Attribute],
          child: LogicalPlan) => 
      {
        ???
      }
      case Filter(condition: Expression, child: LogicalPlan) => 
      {
        ???
      }
      case Intersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean) => 
      {
        ???
      }
      case Except(left: LogicalPlan, right: LogicalPlan, isAll: Boolean) => 
      {
        ???
      }
      case Union(children: Seq[LogicalPlan]) => 
      {
        ???
      }
      case Join(
          left: LogicalPlan,
          right: LogicalPlan,
          joinType: JoinType,
          condition: Option[Expression]) => 
      {
        ???
      }
      case InsertIntoDir(
          isLocal: Boolean,
          storage: CatalogStorageFormat,
          provider: Option[String],
          child: LogicalPlan,
          overwrite: Boolean) => 
      {
        ???
      }
      case View(desc: CatalogTable, output: Seq[Attribute], child: LogicalPlan) => 
      {
        ???
      }
      case With(child: LogicalPlan, cteRelations: Seq[(String, SubqueryAlias)]) => 
      {
        ???
      }
      case WithWindowDefinition(windowDefinitions: Map[String, WindowSpecDefinition], child: LogicalPlan) => 
      {
        ???
      }
      case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) => 
      {
        ???
      }
      case Range(
          start: Long,
          end: Long,
          step: Long,
          numSlices: Option[Int],
          output: Seq[Attribute],
          isStreaming: Boolean) => 
      {
        ???
      }
      case Aggregate(
          groupingExpressions: Seq[Expression],
          aggregateExpressions: Seq[NamedExpression],
          child: LogicalPlan) => 
      {
        ???
      }
      case Window(
          windowExpressions: Seq[NamedExpression],
          partitionSpec: Seq[Expression],
          orderSpec: Seq[SortOrder],
          child: LogicalPlan) => 
      {
        ???
      }
      case Expand(projections: Seq[Seq[Expression]], output: Seq[Attribute], child: LogicalPlan) => 
      {
        ???
      }
      case GroupingSets(
          selectedGroupByExprs: Seq[Seq[Expression]],
          groupByExprs: Seq[Expression],
          child: LogicalPlan,
          aggregations: Seq[NamedExpression]) => 
      {
        ???
      }
      case Pivot(
          groupByExprsOpt: Option[Seq[NamedExpression]],
          pivotColumn: Expression,
          pivotValues: Seq[Expression],
          aggregates: Seq[Expression],
          child: LogicalPlan) => 
      {
        ???
      }
      case GlobalLimit(limitExpr: Expression, child: LogicalPlan) => 
      {
        ???
      }
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) => 
      {
        ???
      }
      case SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan) => 
      {
        ???
      }
      case Sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long,
          child: LogicalPlan) => 
      {
        ???
      }
      case Distinct(child: LogicalPlan) => 
      {
        ???
      }
      case Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan) => 
      {
        ???
      }
      case RepartitionByExpression(
          partitionExpressions: Seq[Expression],
          child: LogicalPlan,
          numPartitions: Int) => 
      {
        ???
      }
      case OneRowRelation() => 
      {
        ???
      }
      case Deduplicate(keys: Seq[Attribute], child: LogicalPlan) => 
      {
        ???
      }
      case leaf:LeafNode =>
      {
        Project(
          leaf.output :+ buildAnnotation(
            plan = leaf,
            rowAnnotation = 
              Literal(false),
            colAnnotations = 
              leaf.output.map { attr => attr.name -> Literal(false) }
          ),
          leaf
        )
      }
    }
    logger.trace(s"ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
    return ret
  }

  def buildAnnotation(
    plan: LogicalPlan,
    rowAnnotation: Expression = null, 
    colAnnotations: Seq[(String, Expression)] = null
  ): NamedExpression = 
  {
    val columns = plan.output.map { _.name }

    // If we're being asked to propagate existing caveats, we'd better
    // be seeing an annotation in the input schema
    assert(
      ((rowAnnotation != null) && (colAnnotations != null)) ||
      columns.exists { _.equals(Caveats.ANNOTATION_COLUMN) }
    )

    val realRowAnnotation: Expression =
      Option(rowAnnotation)
        .getOrElse { Caveats.rowAnnotationExpression }

    val realColAnnotations: Expression =
      Option(colAnnotations)
        .map { cols => 

          // CreateNamedStruct takes parameters in groups of 2: name -> value
          CreateNamedStruct(
            cols.flatMap { case (a, b) => Seq(Literal(a),b) }
          ) 
        }
        .getOrElse { Caveats.allAttributeAnnotationsExpression }

    Alias(
      CreateNamedStruct(Seq(
        Literal(Caveats.ROW_ANNOTATION), realRowAnnotation,
        Literal(Caveats.COLUMN_ANNOTATION), realColAnnotations
      )),
      Caveats.ANNOTATION_COLUMN
    )()
  }
}