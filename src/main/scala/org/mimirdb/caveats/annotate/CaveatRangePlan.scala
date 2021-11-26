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

import org.mimirdb.spark.expressionLogic.{
  wrapAgg,
  isAggregate,
  foldAnd,
  foldOr
}
import org.mimirdb.caveats.boundedtypes.BoundedDataType
import org.mimirdb.utility.GenericObjectPrinter
import org.mimirdb.utility.SparkTreePrinter

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

  def renameExpr(e:Expression, name:String): NamedExpression = CaveatRangeExpression.renameExpr(e,name)

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
        val projBounds = projectList.map { e =>
          {
            val eB = CaveatRangeExpression(e,None,trace)
            e.name -> eB
          }
        }
        val bestGuess = projBounds.map( x => renameExpr(x._2.bg,x._1))
        tlog(s"bestGuess: ${bestGuess}")
        logrewr("PROJECT")
        val annotation =
          buildAnnotation(
            rewrittenChild,
            colAnnotations = projBounds
          )
        val res = Project(
          bestGuess ++ annotation,
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
        val conditionB = CaveatRangeExpression(condition,None,trace)

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
        logrewr("EXCEPT")

        val leftAbounds: Seq[RangeBoundedExpr[NamedExpression]] = CaveatRangeEncoding
          .getNormalAttributesFromNamedExpressions(left.output)
          .map(a => CaveatRangeExpression(a,None,trace)).map { case RangeBoundedExpr(l,b,u) =>
            RangeBoundedExpr(
              l.asInstanceOf[NamedExpression],
              b.asInstanceOf[NamedExpression],
              u.asInstanceOf[NamedExpression],
            )
          }
        val leftNormalAttrs = leftAbounds.map( _.bg)
        val leftRowBounds: RangeBoundedExpr[NamedExpression] = RangeBoundedExpr.fromSeq(CaveatRangeEncoding.rowAnnotationExpressions())
        val rightPrefix = "__RIGHT_"
        val rightAbounds = CaveatRangeEncoding
          .getNormalAttributesFromNamedExpressions(right.output)
          .map(a => CaveatRangeExpression(UnresolvedAttribute(rightPrefix + a.name),None,trace))
        val rightRowBounds = RangeBoundedExpr.fromSeq(CaveatRangeEncoding.rowAnnotationExpressions().map(
          x => UnresolvedAttribute(rightPrefix + x.name)))

        val rowAttrNames = CaveatRangeEncoding.rowAnnotationExpressions().map(x => x.name)

        val rewrRight = tapply(right)
        val renameRightProjects = rewrRight.output.map(
          x =>
          Alias(UnresolvedAttribute(x.name), rightPrefix + x.name)()
        )

        // rewritten children
        val (rewrLeft,rewrRightRenamed) = (
          combineBestGuess(tapply(left)),
          Project(
            renameRightProjects,
            tapply(rewrRight)
          )
        )

        // join on overlap
        val joinCond = foldAnd(leftAbounds.zip(rightAbounds).map{ case (l,r) => l.overlaps(r) }:_*)
        val join = Join(
          rewrLeft,
          rewrRightRenamed,
          Inner,
          Some(joinCond),
          JoinHint.NONE
        )

        // check whether tuples match on best guess values
        //
        // row.lb = max(left.row.lb - sum(right.row.ub), 0)
        // row.bg = max(left.row.bg - sum(if equal(l.bg,r.bg) right.row.bg else 0)
        // row.ub = max(left.row.ub - sum(if iscertainlyequal(l,r) right.row.lb else 0)
        //
        val condBGmatch = foldAnd(leftAbounds.zip(rightAbounds).map { case (l,r) => EqualTo(l.bg,r.bg) }:_*)
        val condCertainMatch = foldAnd(leftAbounds.zip(rightAbounds).map {
          case (l,r) => l.certainlyEqualTo(r)
        }:_*)

        def subtractSumBounds(l: Expression, r: Expression, name: String): NamedExpression = {
          Alias(Greatest(Seq(
            Subtract(l,
              wrapAgg(Sum(r))
            ),
            Literal(0))),
            name
          )()
        }

        val rowBoundExprs: Seq[NamedExpression] = Seq(
          subtractSumBounds(
            leftRowBounds.lb,
            rightRowBounds.ub,
            rowAttrNames(0)),
          subtractSumBounds(
            leftRowBounds.bg,
            CaseWhen(Seq((condBGmatch,rightRowBounds.ub)),Literal(0)),
            rowAttrNames(1)),
          subtractSumBounds(
            leftRowBounds.ub,
            CaseWhen(Seq((condCertainMatch,rightRowBounds.lb)),Literal(0)),
            rowAttrNames(2))
        )
        val groupByExprs = leftNormalAttrs
        val aggExprs = groupByExprs ++ rowBoundExprs

        val agg = Aggregate(
          groupByExprs,
          aggExprs,
          join
        )

        // final filter
        val filter = Filter(
          GreaterThan(UnresolvedAttribute(rowAttrNames(2)),Literal(0)),
          agg
        )

        logop(filter)
        filter
      }

      case Union(children: Seq[LogicalPlan], byName, allowMissingCol) =>
      {
        val rewrittenChildren = children.map(tapply)
        logrewr("UNION")
        val res = Union(rewrittenChildren, byName, allowMissingCol) //TODO should be fine?
        logop(res)
        res
      }

      case j: Join =>
      {
          rewriteJoin(j, trace)
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

      case View(desc: CatalogTable, isTempView: Boolean, child: LogicalPlan) =>
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
      // the rewrite for other operators that are sensitive to sort order have to take this into account
      case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) =>
      {
        ???
      }

      // is a leaf node without any uncertainty
      // case Range(
      //     start: Long,
      //     end: Long,
      //     step: Long,
      //     numSlices: Option[Int],
      //     output: Seq[Attribute],
      //     isStreaming: Boolean) =>
      // {
      //   ???
      // }

      case Aggregate(
          groupingExpressions: Seq[Expression],
          aggregateExpressions: Seq[NamedExpression],
          child: LogicalPlan) =>
        {
          /**
            * Search for group-by expressions in an aggregation result
            * expression and replace them with references to renamed group-by
            * attributes.
            */
          def replaceGroupByInExpression(e: Expression, replacement: Map[Expression,Expression]): Expression = {
            e match {
              case x:AggregateExpression => e
              case x:AggregateFunction => e
              case _ => {
                if(replacement.contains(e))
                  replacement(e)
                else
                  e.withNewChildren(e.children.map(replaceGroupByInExpression(_, replacement)))
              }
            }
          }

          def capOne(e: Expression): Expression = CaseWhen(Seq((GreaterThan(e,Literal(0)),Literal(1))),Literal(0))

          // groupby qggregation?
          val isGroupby = !groupingExpressions.isEmpty

          // rewrite child
          val rewrittenChild = tapply(child)

          // result attribute names (including attributes encoding bounds)
          val resultRangeNames = aggregateExpressions.map ( e => CaveatRangeEncoding.attributeRangeBoundedExpr(e.name) )

          // for aggregation without group-by we do not need to determine group bounds which simplifies the rewrite
          if (!isGroupby) {
            val rowAnnots =  RangeBoundedExpr
              .makeCertain(Literal(1))
              .rename(CaveatRangeEncoding.rowAnnotationAttrNames())
            val resultExprs = aggregateExpressions
              .map(CaveatRangeExpression(_,None,trace))
              .zip(resultRangeNames)
              .map{ case (exprs,names) =>
                exprs.applyToPairs(names, (expr:Expression,name:NamedExpression) => renameExpr(expr, name.name))
              }
            val resultWithFresh =
              resultExprs
              .map( r => r.map(CaveatRangeExpression.withFreshExprIDs(_, trace)) )
              .asInstanceOf[Seq[RangeBoundedExpr[NamedExpression]]]
            val resultBestGuess = resultWithFresh.map(_.bg)
            val resultAttrBounds = resultWithFresh.map( x=> Seq(x.lb,x.ub) ).flatten

            val agg = Aggregate(
              groupingExpressions,
              resultBestGuess ++ rowAnnots.toSeq() ++ resultAttrBounds,
              rewrittenChild
            )

            //tlog("RESULTS\n---------------------\n" +  resultExprs.map( x => SparkTreePrinter.toPrettyJSON(x.bg)).mkString("\n\n"))
            tlog("RESULTS\n---------------------\n" +  resultBestGuess.map( SparkTreePrinter.toPrettyJSON(_)).mkString("\n\n"))
            //tlog("GROUP BY\n-------------------\n" + groupingExpressions.map( _.treeString).mkString("\n\n"))
            //tlog("RESULTS\n-------------------\n" + (resultBestGuess ++ rowAnnots.toSeq() ++ resultAttrBounds).map( _.treeString).mkString("\n\n"))

            logop(agg)
            return agg
          }

          /* Group by expressions and their bounds. We have relatively free choice of how
           * to group range-annotated inputs to output groups. We create one
           * output tuple for each best-guess group (group that exists as the
           * best guess value of some input tuple). All inputs are assigned to
           * an output based on their best guess group-by values. The ranges of
           * the group-by attributes of an output are determined by merging the
           * bounds of all inputs assigned to an output. We realize this by fist
           * calculating the set of outputs and their group-by bounds using
           * aggregation grouping on the best guess group-by values of tuples
           * and calculating the min/max of the lower/upper bounds of the
           * group-by values of all tuples belonging to the same BG group.
           */
          val groupingBoundsGrps = groupingExpressions
          val groupingBnds = groupingExpressions.map ( e => CaveatRangeExpression(e,None,trace) )

          // renamed grouping attributes
          val renamedGroupByNames = Array.range(0,groupingExpressions.length)
            .map(i => "__GROUPBY_" + i.toString())
            .map( a => {
              val bnds = CaveatRangeEncoding.attributeAnnotationAttrName(a)
              Seq(bnds(0), a, bnds(1))
            }
            )

          val renamedGroupByBoundNames = renamedGroupByNames.map { a => CaveatRangeEncoding.attributeRangeBoundedExpr(a(1)) }
          val mergeGroupbyBoundsExprs = groupingBnds.zip(renamedGroupByNames).map { case (re,name) =>
            RangeBoundedExpr(
              Alias(wrapAgg(Min(re.lb)),name(0))(),
              Alias(re.bg,name(1))(),
              Alias(wrapAgg(Max(re.ub)),name(2))()
            )
          }
          val groupAttrPairs = renamedGroupByBoundNames.zip(groupingBnds)

          // group on group-by attributes and merge bounds of group attributes for all tuples with same group-by values
          val groupingMergeGrp = mergeGroupbyBoundsExprs.map( _.bg)
          val groupingMergeAggs = mergeGroupbyBoundsExprs.map( x => Seq(x.lb, x.ub)).flatten

          val mergeGroupBoundsAgg = Aggregate(
            groupingMergeGrp,
            groupingMergeGrp ++ groupingMergeAggs,
            rewrittenChild
          )

          // tlog(s"""GROUP BY:\n----------------------------------------\nORIGINAL GROUP-BY: $groupingBoundsGrps \nBOUNDED EXPRESSIONS: $groupingBnds""")
          // tlog("GROUP BY RENAMED: " + renamedGroupByNames.map(x => x.mkString("(",",",")")).mkString("\n"))

          // join grouping bounds with input
          val joinCond = foldAnd(groupAttrPairs.map { case (newg, g) => newg.overlaps(g) }:_*)
          val joinPossibleGrpMembers = Join(
            mergeGroupBoundsAgg,
            rewrittenChild,
            Inner,
            Some(joinCond),
            JoinHint.NONE
          )

          // rewritten aggregation that calculates aggrgeation function result bounds
          val rowAnnotAtts = CaveatRangeEncoding.rowAnnotationExpressions().zip(
            CaveatRangeEncoding.rowAnnotationAttrNames()
          )
          val certainEqual = foldAnd(groupAttrPairs.map { case (l,r) => l.certainlyEqualTo(r) }:_*)
          val bgEqual = foldAnd(groupAttrPairs.map { case (l,r) => l.bgEqualTo(r) }:_*)

          /*
           * Calulate row annotations as follows:
           *  - for aggregation without group-by exactly one result is returned: (1,1,1)
           *  - for aggreagtion with group-by:
           *     - the group certainly exists if there is at least one tuple
           *       belonging to this group that certainly exists and has certain
           *       group-by values
           *     - the group exists in the best guess world if there is at least
           *       one tuple which belongs to this group in the best guess world
           *     - an upper bound on the maximal multiplicity of the tuple (the
           *       maximum number of groups it represents) is determined by
           *       assuming that for each range annotated input that is assigned
           *       to an output group belong to the group (has the same best
           *       guess group-by values) each of its "duplicates" will end up
           *       forming a separate group. For instance, grouping on A for {
           *       (A: [1,16,20], B:[3,3]) -> [1,2,4], (A: [15,16,50],
           *       B:[3,3]) -> [1,2,5] } a result tuple (A:[1,16,20], ...) may
           *       represent up to 9 = 4 + 5 groups.
           */
          val rowAnnots =
            RangeBoundedExpr(
              Alias(
                wrapAgg(Max(
                  CaseWhen(
                    Seq((certainEqual,capOne(rowAnnotAtts(0)._1))),
                    Literal(0)
                  ))),
                rowAnnotAtts(0)._2
              )(),
              Alias(
                wrapAgg(Max(
                  CaseWhen(
                    Seq((bgEqual,capOne(rowAnnotAtts(1)._1))),
                    Literal(0)
                  ))),
                rowAnnotAtts(1)._2
              )(),
              Alias(
                wrapAgg(Sum(
                  CaseWhen(
                    Seq((bgEqual,rowAnnotAtts(2)._1)),
                    Literal(0)
                  ))),
                rowAnnotAtts(2)._2
              )()
            )

          // generate final expressions
          // we are gouping on group-by attributes and their bounds
          val groupByExprs = renamedGroupByBoundNames.map( _.toSeq).flatten

          /* We construct result expressions from the original result "projection" list by
           *  replacing group-by expressions with references to the renamed
           *  group-by expressions, then applying the range expression rewrites
           *  to get [[RangeBoundedExpr]] (bounded expression triples). The
           *  final result projections are then the best guess values of these
           *  bounded expressions followed by the row annotations computed using
           *  [[rowAnnots]] defined above, and then the bounds for the result
           *  expressions (the lower bound and upper bound expressions from the
           *  results of the expression rewrite.
           */
          tlog("GROUPING PAIRS: " + groupAttrPairs.map{ case (x,y) => x.toString + " => " + y.toString }.mkString("\n"))
          val groupExprToNewNames = groupingExpressions.zip(renamedGroupByBoundNames.map(_.bg)).toMap
          val resultExprs = aggregateExpressions
            .map(replaceGroupByInExpression(_, groupExprToNewNames))
            .map(CaveatRangeExpression(_,Some(groupAttrPairs),trace))
            .zip(resultRangeNames)
            .map{ case (exprs,names) =>
              exprs.applyToPairs(names, (expr:Expression,name:NamedExpression) => renameExpr(expr, name.name))
            }.asInstanceOf[Seq[RangeBoundedExpr[NamedExpression]]]
          val resultBestGuess = resultExprs.map(_.bg)
          val resultAttrBounds = resultExprs.map( x=> Seq(x.lb,x.ub) ).flatten

          tlog("GROUP BY\n-------------------\n" + resultBestGuess.map( _.treeString).mkString("\n\n"))
          tlog("RESULTS\n-------------------\n" + (resultBestGuess ++ rowAnnots.toSeq() ++ resultAttrBounds).map( _.treeString).mkString("\n\n"))

          val agg = Aggregate(
            groupByExprs,
            resultBestGuess ++ rowAnnots.toSeq() ++ resultAttrBounds,
            joinPossibleGrpMembers
          )
          logop(agg)
          agg
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
        val rewrittenChild = tapply(child)
        logrewr("EXPAND")
        val annotation = projections.map { projectList =>
          buildAnnotation(
            rewrittenChild,
            colAnnotations =
              projectList.zip(output).map { case(e,a) =>
                {
                  val eB = CaveatRangeExpression(e,None,trace)
                  a.name -> eB
                }
              }
          )
        }
        val newoutput = output ++ CaveatRangeEncoding.rowAnnotationExpressions().map (x => x.toAttribute) ++ CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(output).map( x => x.toAttribute)

        val res = Expand(
          projections.zip(annotation).map { case (proj,annot) => proj ++ annot },
          newoutput,
          rewrittenChild
        )
        logop(res)
        res
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
      //TODO limits needs to be aware of
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
        PASS_THROUGH_CAVEATS
      }
      // TODO not clear what the semantics of this should be. Can we just do a sample of the rewritten input as a sample of the output. Seems to ignore possible then
      case Sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long,
          child: LogicalPlan) =>
      {
        ???
      }
      // needs be dealt with as an aggregation with only groupby, pass over to aggregation rewrite
      case Distinct(child: LogicalPlan) =>
      {
        tapply(
          Aggregate(
            child.output,
            child.output,
            child
          )
        )
      }
      case Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan) =>
      {
        ???
      }
      case RepartitionByExpression(
          partitionExpressions: Seq[Expression],
          child: LogicalPlan,
          numPartitions: Option[Int]) =>
      {
        ???
      }
      // a single row that has no attributes (dealt with like other leaf relations)
      // case OneRowRelation() =>
      // {
      //   ???
      // }

      //TODO only works for deduplication on all attributes. Otherwise, we need to encode uncertainty in the selected tuples
      case Deduplicate(keys: Seq[Attribute], child: LogicalPlan) =>
      {
        logrewr("DEDUPLICATION")
        // deduplication on all attributes dealt by rewriting it into
        // aggregation with group-by on all attributes, but without any
        // aggregation functions.
        if (keys.equals(child.output)) {
          val groupby = child.output
          val replaceAgg = Aggregate(groupby, groupby, child)
          val rewrReplacementAgg = tapply(replaceAgg)
          rewrReplacementAgg
        }
        // deduplication on a subset of the attributes as keys results picks the first tuple with a particular key which is sensitive to sort order.
        //
        else {
          ???
        }
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
                RangeBoundedExpr.makeCertain(UnresolvedAttribute(attr.name).asInstanceOf[Expression])) }}
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
    plan: Join,
    trace: Boolean = false
  ): LogicalPlan =
    plan match {
      case Join(left,right,Cross,c,h) => {
        rewriteInnerJoin(plan,trace)
      }
      case Join(left,right,Inner,c,h) => {
        rewriteInnerJoin(plan,trace)
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
    plan: Join,
    trace: Boolean = false
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
              .map { case (x,y) => renameExpr(x, y.name) }
        val rightAnnotProjections =
          (CaveatRangeEncoding.rowAnnotationExpressions() ++
            CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(right.output))
              .zip(
                CaveatRangeEncoding.rowAnnotationExpressions(RIGHT_ANNOT_PREFIX) ++
                CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(right.output, RIGHT_ANNOT_PREFIX)
                )
            .map { case (x,y) => renameExpr(x, y.name) }
        val leftUnresolved = left.output.map(CaveatRangeExpression.unresolveAttrsInExpr(_))
        val rightUnresolved = right.output.map(CaveatRangeExpression.unresolveAttrsInExpr(_))

        val rewrLeft = Project(
          leftUnresolved ++ leftAnnotProjections,
          apply(left)
        )
        val rewrRight = Project(
          rightUnresolved ++ rightAnnotProjections,
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
        val conditionB = condition.map(CaveatRangeExpression.applynogrp(_,trace))

        // replace references to ANNOTATION_ATTRIBUTE with the left or the right version
        val conditionBadapted = conditionB.map( x => RangeBoundedExpr(
          CaveatRangeExpression.replaceAnnotationAttributeReferences(x.lb, attrMap),
          CaveatRangeExpression.replaceAnnotationAttributeReferences(x.bg, attrMap),
          CaveatRangeExpression.replaceAnnotationAttributeReferences(x.ub, attrMap)
        ))

        // if join condition is possibly true for a tuple pair, then we have to return the tuple pair
        val rewrittenCondition = conditionBadapted.map(x => x.ub)

        // map join condition expression results to N (row annotation), if there is no join condition then return (1,1,1)
        val annotConditionToRow: RangeBoundedExpr[Expression] = conditionBadapted
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
            ).asInstanceOf[Expression]
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

  /**
    * Group input based on best-guess values and merge row annotations.
    *
    * @param plan input query
    * @returns root operator of the addtiional instrumentation added on top of the input plan
    */
  def combineBestGuess(plan: LogicalPlan): LogicalPlan = {
    val inputAttrs = CaveatRangeEncoding.getNormalAttributesFromNamedExpressions(plan.output)
    val groupBy = inputAttrs
    // val boundAggs = plan.output.map { a => CaveatRangeEncoding.attributeAnnotationExpressions(a.name) }
    val gbOutputs = inputAttrs.map( x => Alias(UnresolvedAttribute(x.name), x.name)() )

    val rowAnnotAggs = CaveatRangeEncoding.rowAnnotationExpressions()
      .map( x => Alias(wrapAgg(Sum(x)), x.name)())

    val attrBoundAggs = inputAttrs.map { x =>
      val name = x.name
      val lb = CaveatRangeEncoding.attrLBexpression(name)
      val ub = CaveatRangeEncoding.attrUBexpression(name)
      val lbName = CaveatRangeEncoding.attributeAnnotationAttrName(name)(0)
      val ubName = CaveatRangeEncoding.attributeAnnotationAttrName(name)(1)
      Seq(
        Alias(wrapAgg(Min(lb)), lbName)(),
        Alias(wrapAgg(Max(ub)), ubName)()
      )
    }.flatten

    Aggregate(
      inputAttrs,
      inputAttrs ++ rowAnnotAggs ++ attrBoundAggs,
      plan
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
    rowAnnotation: RangeBoundedExpr[Expression] = null,
    colAnnotations: Seq[(String,RangeBoundedExpr[Expression])] = null,
    projExprs: Seq[NamedExpression] = null,
    annotationAttr: String = Constants.ANNOTATION_ATTRIBUTE,
    outputAnnotationAttr: String = Constants.ANNOTATION_ATTRIBUTE
  )
      : LogicalPlan =
  {
    val pExprs =
      if (projExprs == null) {
        CaveatRangeEncoding.getNormalAttributesFromNamedExpressions(plan.output)
      }
      else projExprs

    Project(
      pExprs ++ buildAnnotation(plan, rowAnnotation, colAnnotations, annotationAttr, outputAnnotationAttr),
      plan
    )
  }

  /**
    * Rewrites for translating other representations of uncertainty into UAADBs.
    */
  override def translateFromUncertaintyModel(plan: LogicalPlan, model: UncertaintyModel) =
    model match {
      case TupleIndependentProbabilisticDatabase(probAttr) => {
        tipToRange(plan, probAttr)
      }
      case XDB(idAttr, probAttr) => {
        xdbToRange(plan, idAttr, probAttr)
      }
      case CoddTable() => {
        coddToRange(plan)
      }
    }

  /**
    * Translate TIP relation into RangeEncoding (probability as attribute P)
    *  - All attributes are certain
    *  - tuples are possible if P != 0
    *  - tuples are best guess if P >= 0.5
    *  - tuples are certain if P == 1.0
    */
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
        .map { a => a -> RangeBoundedExpr.makeCertain(UnresolvedAttribute(a).asInstanceOf[Expression]) },
      projExprs = normalAttrs
    )
  }

  def xdbToRange(plan: LogicalPlan, idAttr: String, probAttr: String): LogicalPlan = {
    ???
  }

  def coddToRange(plan: LogicalPlan): LogicalPlan = {
    constructAnnotUsingProject(plan,
      rowAnnotation = RangeBoundedExpr.makeCertain(Literal(1)),
      colAnnotations = plan.output.map ( a => a.name)
        .map { a => a -> RangeBoundedExpr.makeCertain(UnresolvedAttribute(a).asInstanceOf[Expression]) },
      projExprs = plan.output.map{ a =>
        {
          //TODO for now we fail if an input data type is not bounded
          assert(BoundedDataType.isBoundedType(a.dataType))
          val median = BoundedDataType.domainMinMedianMax(a.dataType)
          renameExpr(CaseWhen(
            Seq((IsNull(UnresolvedAttribute(a.name)),Literal(median))),
            UnresolvedAttribute(a.name)),
            a.name)
        }
      }
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
    rowAnnotation: RangeBoundedExpr[Expression] = null,
    colAnnotations: Seq[(String, RangeBoundedExpr[Expression])] = null,
    annotationAttr: String = Constants.ANNOTATION_ATTRIBUTE,
    outputAnnotationAttr: String = Constants.ANNOTATION_ATTRIBUTE
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
          renameExpr(x.lb, CaveatRangeEncoding.rowAnnotationAttrNames(annotationAttr)(0)),
          renameExpr(x.bg, CaveatRangeEncoding.rowAnnotationAttrNames(annotationAttr)(1)),
          renameExpr(x.ub, CaveatRangeEncoding.rowAnnotationAttrNames(annotationAttr)(2))
          )
        }}
        .getOrElse {
          if (annotationAttr == outputAnnotationAttr)
            CaveatRangeEncoding.rowAnnotationExpressions(annotationAttr).map( x => selfProject(x))
          else
            CaveatRangeEncoding.rowAnnotationExpressions(annotationAttr).zip(
              CaveatRangeEncoding.rowAnnotationExpressions(outputAnnotationAttr))
          .map { case (in,out) => renameExpr(in, out.name) }
        }

    val realColAnnotations: Seq[NamedExpression] =
      Option(colAnnotations)
        .map { col =>
          col.flatMap { case (a, x) =>
            Seq(
              renameExpr(x.lb, CaveatRangeEncoding.attributeAnnotationAttrName(a, annotationAttr)(0)),
              renameExpr(x.ub, CaveatRangeEncoding.attributeAnnotationAttrName(a, annotationAttr)(1))
            )
          }
        }
        .getOrElse {
          if (annotationAttr == outputAnnotationAttr)
            CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(normalAttributes)
          else
            CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(normalAttributes,annotationAttr).zip(
              CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(normalAttributes,outputAnnotationAttr))
              .map { case (in,out) => renameExpr(in, out.name) }
        }

    { CaveatRangeEncoding.allAttributeAnnotationsExpressionsFromExpressions(normalAttributes) }

    realRowAnnotations ++ realColAnnotations
  }

  private def selfProject(a: NamedExpression): NamedExpression =
    renameExpr(a, a.name)

}
