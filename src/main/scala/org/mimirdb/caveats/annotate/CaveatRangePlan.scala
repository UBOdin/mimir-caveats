package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.AliasIdentifier

import org.apache.spark.sql.catalyst.plans._

import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.caveats._
import org.mimirdb.caveats.Constants._

/**
  * Instrument a [LogicalPlan] to generate and propagate [CaveatRangeType] annotation.
  * In the result produced by the rewritten plan every row is annotated with a triple
  *  (lb,bg,ub) which records the minimum (lb) and maximum (ub) multiplicity of the
  * tuple across all possible worlds. bg encodes the encodes the multiplicity of the tuple
  *  in the selected best guess world. Furthermore, for each attribute A of the schema
  *  of the input plan we record an upper and a lower bound on the value of this attribute
  *  across all possible worlds.
  */
object CaveatRangePlan
  extends AnnotationInstrumentationStrategy
  with LazyLogging
{

  def annotationEncoding = CaveatRangeEncoding

  def annotationType = CaveatRangeType

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

      /*********************************************************/
      // do not rewrite plan that already has caveats
      case _ if Caveats.planIsAnnotated(plan) => plan

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
              projectList.map { e =>
                {
                  val eB = CaveatRangeExpression(e)
                  e.name -> (eB._1, eB._3)
                }
              }
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
        val rewrittenChild = apply(child)
        val conditionB = CaveatRangeExpression(condition)

        // need to map (Boolean, Booelan, Boolean) of condition to (Int,Int,Int)
        // using False -> 0 and True -> 1
        // then multiply with tuple annotation
        // only keep tuples for which condition evaluates to (_,_,True)
        val newFilter = Filter(
          conditionB._3,
          rewrittenChild
        )

        val projectionExprs = newFilter.output :+ buildAnnotation(
            plan = newFilter,
            rowAnnotation =
              CaveatRangeExpression.booleanToRowAnnotation(conditionB)
          )

        Project(
          projectionExprs,
          newFilter
        )
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
        val rewrittenChildren = children.map(apply)
        Union(rewrittenChildren) //TODO should be fine?
      }
      case j: Join =>
      {
          rewriteJoin(j)
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
      //TODO technically, we have to reason about all possible sort orders. If
      //we materialize the position of a row as an attribute (and adapt parents
      //accordingly) then this may work, the attribute annotation on this column
      //would be the lowest and highest possible position of the tuple in sort
      //order
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
          /*
          A classical aggregate.

          Something to note here is that groupingExpressions is only used to
          define the set of group-by attributes.  aggregateExpressions is the
          actual projection list, and may include fragments to be evaluated both
          pre- and post-aggregate.
         */
        ???
      }
      // for window operators we face the same challenge as for sort, we have to provide bounds for the aggregation function across all possible windows (partition + sort order)
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
      // create tuple annotation [1,1,1] and for each attribute A create (A,A) as the attribute bound
      case leaf:LeafNode =>
      {
        Project(
          leaf.output :+ buildAnnotation(
            plan = leaf,
            rowAnnotation =
              (Literal(1), Literal(1), Literal(1)),
            colAnnotations =
              leaf.output.map { attr => { (attr.name,  (UnresolvedAttribute(attr.name), UnresolvedAttribute(attr.name))) }}
          ),
          leaf
        )
      }
    }
    logger.trace(s"RANGE-ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
    return ret
  }

  /**
    *  Dispatch to individual rewriters for join types
    */
  private def rewriteJoin(
    plan: Join
  ): LogicalPlan =
    plan match {
      case Join(left,right,Cross,c,h) => {
        rewriteInnerJoin(plan)
      }
      case Join(left,right,Inner,c,h) => {
        rewriteInnerJoin(plan)
      }
      case Join(left,right,ExistenceJoin(_),c,h) => { // need to be dealt with separatly?
        ???
      }
      case Join(left,right,LeftOuter,c,h) => {
        ???
      }
      case Join(left,right,RightOuter,c,h) => {
        ???
      }
      case Join(left,right,FullOuter,c,h) => {
        ???
      }
      case Join(left,right,LeftSemi,c,h) => {
        ???
      }
      case Join(left,right,LeftAnti,c,h) => {
        ???
      }
      case Join(left,right,UsingJoin(_,_),c,h) => {
        ???
      }
      case Join(left,right,NaturalJoin(_),c,h) => { //need to match for subtypes (inner, or left, right, full outer)
        ???
      }
    }

  /** Instruments an inner join for range caveats.
    *
    *  @returns
    */
  private def rewriteInnerJoin(
    plan: Join
  ): LogicalPlan =
    plan match { case Join(left,right,joinType,condition,hint) =>
      {
          // rename the annotation attribute from the left and right input
          val LEFT_ANNOT_ATTR = ANNOTATION_ATTRIBUTE + "_LEFT"
          val RIGHT_ANNOT_ATTR = ANNOTATION_ATTRIBUTE+"_RIGHT"
          val rewrLeft = Project(
            left.output :+ Alias(
              UnresolvedAttribute(ANNOTATION_ATTRIBUTE),
              LEFT_ANNOT_ATTR)(),
            apply(left)
          )
          val rewrRight = Project(
            left.output :+ Alias(
              UnresolvedAttribute(ANNOTATION_ATTRIBUTE),
              RIGHT_ANNOT_ATTR)(),
            apply(right)
          )

          // store which attribute corresponds to which annotation attribute
          val attrMap = (left.output.map( x => x.name -> LEFT_ANNOT_ATTR) ++
                        right.output.map( x => x.name -> RIGHT_ANNOT_ATTR)).toMap

          // rewrite condition
          val conditionB = condition.map(CaveatRangeExpression.apply)

          // if join condition is possibly true for a tuple pair, then we have to return the tuple pair
          val rewrittenCondition = conditionB.map(x => x._3)

          // replace references to ANNOTATION_ATTRIBUTE with the left or the right version
          val conditionBadapted = conditionB.map(x => x match {
            case (lb,bg,ub) =>
              (
                CaveatRangeExpression.replaceAnnotationAttributeReferences(lb, attrMap),
                CaveatRangeExpression.replaceAnnotationAttributeReferences(bg, attrMap),
                CaveatRangeExpression.replaceAnnotationAttributeReferences(ub, attrMap)
              )
          }
          )

          // map join condition expression results to N (row annotation), if there is no join condition then return (1,1,1)
          val annotConditionToRow :(Expression,Expression,Expression) = conditionBadapted.map(x =>
            CaveatRangeExpression.applyPointwiseToTuple3(
              CaveatRangeExpression.boolToInt,
              x)
          ).getOrElse(CaveatRangeExpression.neutralRowAnnotation)

          // non-annotation attributes
          val attributes =
            left.output.map  { (LEFT_ANNOT_ATTR, _)  } ++
            right.output.map { (RIGHT_ANNOT_ATTR, _) }

          // join rewritten inputs after renaming the join attribute
          val rewrittenJoin = Join(rewrLeft,
            rewrRight,
            joinType,
            rewrittenCondition,
            JoinHint.NONE
          )

          // multiply row annotations and multply the result with the join condition result mapped as 0 or 1
          val rowAnnotations = (
            Multiply(
              Multiply(
                CaveatRangeEncoding.rowLBexpression(LEFT_ANNOT_ATTR),
                CaveatRangeEncoding.rowLBexpression(RIGHT_ANNOT_ATTR)
              ),
              annotConditionToRow._1
            ),
            Multiply(
              Multiply(
                CaveatRangeEncoding.rowBGexpression(LEFT_ANNOT_ATTR),
                CaveatRangeEncoding.rowUBexpression(RIGHT_ANNOT_ATTR)
              ),
              annotConditionToRow._2
            ),
            Multiply(
              Multiply(
                CaveatRangeEncoding.rowUBexpression(LEFT_ANNOT_ATTR),
                CaveatRangeEncoding.rowUBexpression(RIGHT_ANNOT_ATTR)
              ),
              annotConditionToRow._3
            )
          )

          val annotations = buildAnnotation(
            plan = rewrittenJoin,
            rowAnnotation = rowAnnotations,
            colAnnotations = null //TODO
          )

          // calculate multiplicities
          Project(
            rewrittenJoin.output :+ annotations,
            rewrittenJoin
          )
      }
    }


  /** Group input based on best-guess values and merge row annotations.
    *
    * @param plan input query
    * @returns root operator of the addtiional instrumentation added on top of the input plan
    */
  def combineBestGuess(plan: LogicalPlan): LogicalPlan = {
    val inputAttrs = plan.output.filter( _.name != Constants.ANNOTATION_ATTRIBUTE ).map( x => x.name)
    val groupBy = plan.output
    val boundAggs = plan.output.map { a => CaveatRangeEncoding.attributeAnnotationExpression(a.name) }

    // sum up the tuple annotations
    val boundFlatAttrs = Seq("__LB", "__BG", "__UB").map( x => Constants.ROW_FIELD + x )
    val rowAnnotAggs : Seq[NamedExpression] =
      CaveatRangeEncoding.rowAnnotationExpressionTriple()
        .productIterator.toSeq.asInstanceOf[Seq[Expression]]
        .zip(boundFlatAttrs).map {
          x => x match { case (bound:Expression,name:String) => Alias(Sum(bound), name)() }
        }

    // merge attribute bounds
    val boundAttrFlatAttrs: Seq[String] = inputAttrs.map( x => { Seq("__LB", "__UB").map( y => x + y) }).flatten
    val attrAnnotAggs = inputAttrs.map( x => CaveatRangeEncoding.attributeAnnotationExpression(x) )
      .map( x => Seq(Min(CaveatRangeEncoding.lbExpression(x)), Max(CaveatRangeEncoding.ubExpression(x))) : Seq[Expression] )
      .flatten
      .zip(boundAttrFlatAttrs)
      .map(x => x match { case (bound:Expression,name:String) => Alias(bound, name)()  } )

    val nestAnnotationAttr =
      inputAttrs.map ( x => UnresolvedAttribute(x) )

    // create aggregation for grouping and then renest into annotation attribute
    constructAnnotUsingProject(
      Aggregate(
        groupBy,
        groupBy ++ attrAnnotAggs ++ rowAnnotAggs,
        plan
      ),
      boundFlatAttrs.map( x => UnresolvedAttribute(x) ) match { case Seq(lb,bg,ub) => (lb,bg,ub) },
      inputAttrs.map( x =>
        (x,
        { Seq("__LB", "__UB").map( y => UnresolvedAttribute(x + y) ) } match { case Seq(a,b) => (a,b) }
        )
      )
    )
  }

  //TODO this should exist in standard lib?
  private def grptwo[T]( a: Seq[T] ) : Seq[(T,T)] =
    a match {
      case Nil => Seq()
      case b :: c :: remainder => Seq((b,c)) ++ grptwo(remainder)
    }

  def foldAdd(exprs: Seq[Expression]) : Expression = {
    exprs match {
      case Seq(e) => e
      case _ => exprs.foldLeft[Expression](Literal(0))((x,y) => Add(x,y))
    }
  }

  def foldMult(exprs: Seq[Expression]) : Expression = {
    exprs match {
      case Seq(e) => e
      case _ => exprs.foldLeft[Expression](Literal(1))((x,y) => Multiply(x,y))
    }
  }

  def constructAnnotUsingProject(
    plan: LogicalPlan,
    rowAnnotation: (Expression,Expression,Expression),
    colAnnotations: Seq[(String,(Expression,Expression))],
    annotationAttr: String = Constants.ANNOTATION_ATTRIBUTE)
      : LogicalPlan =
  {
    Project(
      plan.output :+ buildAnnotation(plan, rowAnnotation, colAnnotations, annotationAttr),
      plan
    )
  }

  /** Build up a value of the nested annotation attribute from its components.
   *
   *  @param plan the operator whose schema we are annotating
   *  @param rowAnnotation the multiplicity annotation for the row
   *  @param colAnnotations the annotations for all columns of the result schema of [plan]
   *  @param annotationAttr the name of the annotation attribute for the plan (input)
   */
  def buildAnnotation(
    plan: LogicalPlan,
    rowAnnotation: (Expression, Expression, Expression) = null,
    colAnnotations: Seq[(String, (Expression,Expression))] = null,
    annotationAttr: String = Constants.ANNOTATION_ATTRIBUTE
  ): NamedExpression =
  {
    val columns = plan.output.map { _.name }

    // If we're being asked to propagate existing caveats, we'd better
    // be seeing an annotation in the input schema
    assert(
      ((rowAnnotation != null) && (colAnnotations != null)) ||
      columns.exists { _.equals(Constants.ANNOTATION_ATTRIBUTE) }
    )

    val realRowAnnotation: Expression =
      Option(rowAnnotation).map { case (lb,bg,ub) =>
        {
        CreateNamedStruct(Seq(
          Literal(LOWER_BOUND_FIELD), lb,
          Literal(BEST_GUESS_FIELD), bg,
          Literal(UPPER_BOUND_FIELD), ub
        ))
      }}
        .getOrElse { CaveatRangeEncoding.rowAnnotationExpression(annotationAttr) }

    val realColAnnotations: Expression =
      Option(colAnnotations)
        .map { cols =>

          // CreateNamedStruct takes parameters in groups of 2: name -> value
          CreateNamedStruct(
            cols.flatMap { case (a, (lb,ub)) =>
              Seq(Literal(a),
                CreateNamedStruct(Seq(
                  Literal(LOWER_BOUND_FIELD), lb,
                  Literal(UPPER_BOUND_FIELD), ub
                ))
              )
            }
          )
        }
        .getOrElse { CaveatRangeEncoding.allAttributeAnnotationsExpression(annotationAttr) }

    Alias(
      CreateNamedStruct(Seq(
        Literal(ROW_FIELD), realRowAnnotation,
        Literal(ATTRIBUTE_FIELD), realColAnnotations
      )),
      ANNOTATION_ATTRIBUTE
    )()
  }
}
