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
class CaveatRangePlan()
  extends AnnotationInstrumentationStrategy
  with LazyLogging
{

  def outputEncoding = CaveatRangeEncoding

  def annotationType = CaveatRangeType

  /**
   * Return a logical plan identical to the input plan, but with an additional
   * column containing caveat annotations.
   */
  def apply(plan: LogicalPlan, trace: Boolean = false): LogicalPlan =
  {
    def tapply(plan: LogicalPlan): LogicalPlan = apply(plan, trace)

    def tlog(message: String) = { if (trace) println(message)  }
    def logop(o: LogicalPlan) = { if (trace) println("--------------------------\nREWRITTEN OPERATOR:\n--------------------------\n" + o.toString) }
    def logrewr(typ: String) = { if (trace) println(s"========================================\nREWRITE OPERATOR TYPE $typ\n========================================") }

    def PASS_THROUGH_CAVEATS = plan.mapChildren {
      logrewr("PASS THROUGH")
      tapply(_)
    }

    // Each operator has its own interactions with caveats.  Force an explicit
    // matching rather than using Spark's tree recursion operators and default
    // to fail-stop operation to make sure that new operators or non-standard
    // behaviors get surfaced early.
    tlog(s"REWRITING PLAN OPERATOR: $plan")

    val ret = plan match {

      /*********************************************************/
      // do not rewrite plan that already has caveats
      case _ if outputEncoding.isValidAnnotatedNamedExpressionSchema(plan.output) => {
        tlog("operator is already rewritten")
        plan
      }

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
        val rewrittenChild = tapply(child)
        logrewr("PROJECT")
        val annotation =
          buildAnnotation(
            rewrittenChild,
            colAnnotations =
              projectList.map { e =>
                {
                  val eB = CaveatRangeExpression(e)
                  e.name -> eB
                }
              }
          )
        val res = Project(
          projectList ++ annotation,
          rewrittenChild
        )
        logop(res)
        res
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
        val rewrittenChild = tapply(child)
        logrewr("FILTER")
        val conditionB = CaveatRangeExpression(condition)

        // need to map (Boolean, Booelan, Boolean) of condition to (Int,Int,Int)
        // using False -> 0 and True -> 1
        // then multiply with tuple annotation
        // only keep tuples for which condition evaluates to (_,_,True)
        val newFilter = Filter(
          conditionB.ub,
          rewrittenChild
        )

        val projectionExprs: Seq[NamedExpression] =
          CaveatRangeEncoding
            .getNormalAttributesFromNamedExpressions(newFilter.output,ANNOTATION_ATTRIBUTE) ++
          buildAnnotation(
            plan = newFilter,
            rowAnnotation =
              CaveatRangeExpression.booleanToRowAnnotation(conditionB)
          )

        val res = Project(
          projectionExprs,
          newFilter
        )
        logop(res)
        res
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
        val rewrittenChildren = children.map(tapply)
        logrewr("UNION")
        val res = Union(rewrittenChildren) //TODO should be fine?
        logop(res)
        res
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
        logrewr("LEAF NODE")
        val res = Project(
          leaf.output ++ buildAnnotation(
            plan = leaf,
            rowAnnotation = CaveatRangeExpression.neutralRowAnnotation(),
            colAnnotations =
              leaf.output.map { attr => { (attr.name,
                RangeBoundedExpr.makeCertain(UnresolvedAttribute(attr.name))) }}
          ),
          leaf
        )
        logop(res)
        res
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
        val LEFT_ANNOT_PREFIX = ANNOTATION_ATTRIBUTE + "_LEFT"
        val RIGHT_ANNOT_PREFIX = ANNOTATION_ATTRIBUTE + "_RIGHT"
        // projections that rename join input annotation attributes
        val leftAnnotProjections =
          (CaveatRangeEncoding.rowAnnotationExpressions() ++
            CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(left.output))
              .zip(
                CaveatRangeEncoding.rowAnnotationExpressions(LEFT_ANNOT_PREFIX) ++
                CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(left.output, LEFT_ANNOT_PREFIX)
                )
              .map { case (x,y) => Alias(x, y.name)() }
        val rightAnnotProjections =
          (CaveatRangeEncoding.rowAnnotationExpressions() ++
            CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(right.output))
              .zip(
                CaveatRangeEncoding.rowAnnotationExpressions(RIGHT_ANNOT_PREFIX) ++
                CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(right.output, RIGHT_ANNOT_PREFIX)
                )
              .map { case (x,y) => Alias(x, y.name)() }
        val rewrLeft = Project(
          left.output ++ leftAnnotProjections,
          apply(left)
        )
        val rewrRight = Project(
          right.output ++ rightAnnotProjections,
          apply(right)
        )

        // map normal attributes to inputs
        val normalAttrMap = (
          left.output.map{ x => x.name -> LEFT_ANNOT_PREFIX }
            ++ right.output.map{ x => x.name -> RIGHT_ANNOT_PREFIX }
        ).toMap

        // map input annotation attribute names to renamed names
        val attrMap = (
          leftAnnotProjections.map { case Alias(proj: NamedExpression, name) => proj.name -> name case _ =>  null } ++
            rightAnnotProjections.map { case Alias(proj: NamedExpression, name) => proj.name -> name case _ =>  null }
        ).toMap

        // rewrite condition
        val conditionB = condition.map(CaveatRangeExpression.apply)

        // replace references to ANNOTATION_ATTRIBUTE with the left or the right version
        val conditionBadapted = conditionB.map( x => RangeBoundedExpr(
          CaveatRangeExpression.replaceAnnotationAttributeReferences(x.lb, attrMap),
          CaveatRangeExpression.replaceAnnotationAttributeReferences(x.bg, attrMap),
          CaveatRangeExpression.replaceAnnotationAttributeReferences(x.ub, attrMap)
        ))

        // if join condition is possibly true for a tuple pair, then we have to return the tuple pair
        val rewrittenCondition = conditionBadapted.map(x => x.ub)

        // map join condition expression results to N (row annotation), if there is no join condition then return (1,1,1)
        val annotConditionToRow: RangeBoundedExpr = conditionBadapted
          .map(CaveatRangeExpression.booleanToRowAnnotation)
          .getOrElse(CaveatRangeExpression.neutralRowAnnotation())

        // non-annotation attributes
        val attributes =
          left.output.map  { (LEFT_ANNOT_PREFIX, _)  } ++
          right.output.map { (RIGHT_ANNOT_PREFIX, _) }

        // join rewritten inputs after renaming the join attribute
        val rewrittenJoin = Join(rewrLeft,
          rewrRight,
          joinType,
          rewrittenCondition,
          JoinHint.NONE
        )

        // multiply row annotations and multply the result with the join condition result mapped as 0 or 1
        val normalAttrs = rewrittenJoin.output
          .filterNot(x => x.name.startsWith(LEFT_ANNOT_PREFIX) || x.name.startsWith(RIGHT_ANNOT_PREFIX))

        val rowAnnotations = RangeBoundedExpr.fromSeq(annotConditionToRow
          .zip(Seq(
            CaveatRangeEncoding.rowLBexpression(_),
            CaveatRangeEncoding.rowBGexpression(_),
            CaveatRangeEncoding.rowUBexpression(_))
          )
          .map { case (x,f) =>
            Multiply(
              Multiply(
                f(LEFT_ANNOT_PREFIX),
                f(RIGHT_ANNOT_PREFIX)
              ),
              x
            )
          }
        )

        val colAnnotations = normalAttrMap.map { case (a,annPrefix) =>
          a ->
          RangeBoundedExpr.fromBounds(
            CaveatRangeEncoding.attrLBexpression(a,annPrefix),
            CaveatRangeEncoding.attrUBexpression(a,annPrefix)
          )
        }.toSeq

        val annotations = buildAnnotation(
          plan = rewrittenJoin,
          rowAnnotation = rowAnnotations,
          colAnnotations = colAnnotations
        )


        // calculate multiplicities
        Project(
          normalAttrs ++ annotations,
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
    val boundAggs = plan.output.map { a => CaveatRangeEncoding.attributeAnnotationExpressions(a.name) }

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
    val attrAnnotAggs = inputAttrs.map( x => CaveatRangeEncoding.attributeAnnotationExpressions(x) )
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
      boundFlatAttrs.map( x => UnresolvedAttribute(x) ) match { case Seq(lb,bg,ub) => RangeBoundedExpr(lb,bg,ub) },
      inputAttrs.map( x =>
        (x,
        { Seq("__LB", "__UB").map( y => UnresolvedAttribute(x + y) ) } match { case Seq(a,b) => RangeBoundedExpr.fromBounds(a,b) }
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
    rowAnnotation: RangeBoundedExpr = null,
    colAnnotations: Seq[(String,RangeBoundedExpr)] = null,
    projExprs: Seq[NamedExpression] = null,
    annotationAttr: String = Constants.ANNOTATION_ATTRIBUTE)
      : LogicalPlan =
  {
    val pExprs = if (projExprs == null) { plan.output.filterNot( _.name == annotationAttr) } else projExprs
    Project(
      pExprs ++ buildAnnotation(plan, rowAnnotation, colAnnotations, annotationAttr),
      plan
    )
  }

  override def translateFromUncertaintyModel(plan: LogicalPlan, model: UncertaintyModel) =
    model match {
      case TupleIndependentProbabilisticDatabase(probAttr) => {
        tipToRange(plan, probAttr)
      }
      case XDB(idAttr, probAttr) => {
        xdbToRange(plan, idAttr, probAttr)
      }
    }

  def tipToRange(plan: LogicalPlan, probAttr: String): LogicalPlan = {
    assert(plan.output.exists(_.name == probAttr))
    val normalAttrs = CaveatRangeEncoding.getNormalAttributesFromNamedExpressions(
      plan.output.filterNot( _.name == probAttr))
    val normalAttrNames = normalAttrs.map(_.name)
    constructAnnotUsingProject(plan,
      rowAnnotation = RangeBoundedExpr(
        // certain only with 1.0 probability
        CaseWhen(Seq((EqualTo(UnresolvedAttribute(probAttr), Literal(1.0)), Literal(1))), Literal(0)),
        // best guess when probability >= 0.5
        CaseWhen(Seq((GreaterThan(UnresolvedAttribute(probAttr), Literal(0.5)), Literal(1))), Literal(0)),
        Literal(1)
      ),
      colAnnotations = normalAttrNames
        .map { a => a -> RangeBoundedExpr.makeCertain(UnresolvedAttribute(a)) },
      projExprs = normalAttrs
    )
  }

  def xdbToRange(plan: LogicalPlan, idAttr: String, probAttr: String): LogicalPlan = {
    ???
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
    rowAnnotation: RangeBoundedExpr = null,
    colAnnotations: Seq[(String, RangeBoundedExpr)] = null,
    annotationAttr: String = Constants.ANNOTATION_ATTRIBUTE
  ): Seq[NamedExpression] =
  {
    val columns = plan.output
    val normalAttributes = CaveatRangeEncoding
      .getNormalAttributesFromNamedExpressions(plan.output, annotationAttr)

    // If we're being asked to propagate existing caveats, we'd better
    // be seeing an annotation in the input schema
    assert(
      ((rowAnnotation != null) && (colAnnotations != null)) ||
        CaveatRangeEncoding.isValidAnnotatedNamedExpressionSchema(columns),
      "no annotation expressions provided and not a valid annotated schema " + columns.toString()
    )

    val realRowAnnotations: Seq[NamedExpression] =

      Option(rowAnnotation).map { x =>
        {
          Seq(
          Alias(x.lb, CaveatRangeEncoding.rowAnnotationAttrNames(annotationAttr)(0))(),
          Alias(x.bg, CaveatRangeEncoding.rowAnnotationAttrNames(annotationAttr)(1))(),
          Alias(x.ub, CaveatRangeEncoding.rowAnnotationAttrNames(annotationAttr)(2))()
          )
        }}
        .getOrElse { CaveatRangeEncoding.rowAnnotationExpressions(annotationAttr).map( x => selfProject(x)) }

    val realColAnnotations: Seq[NamedExpression] =
      Option(colAnnotations)
        .map { col =>
          col.flatMap { case (a, x) =>
            Seq(
              Alias(x.lb, CaveatRangeEncoding.attributeAnnotationAttrName(a, annotationAttr)(0))(),
              Alias(x.ub, CaveatRangeEncoding.attributeAnnotationAttrName(a, annotationAttr)(1))()
            )
          }
        }
        .getOrElse { CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(normalAttributes) }

    realRowAnnotations ++ realColAnnotations
  }

  private def selfProject(a: NamedExpression): NamedExpression =
    Alias(a, a.name)()

}
