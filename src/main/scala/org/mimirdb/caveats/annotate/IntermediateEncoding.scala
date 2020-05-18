package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  Project
}
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  NamedExpression,
  Attribute,
  AttributeReference,
  Alias
}
import org.mimirdb.spark.expressionLogic.foldOr

trait IntermediateEncoding
{
  def annotationForRow: NamedExpression
  def annotationFor(attr: Attribute): NamedExpression

  def getAnnotationExpressions(
    oldPlan: LogicalPlan,
    newChild: LogicalPlan,
    attributes: Seq[(Attribute, Expression)],
    row: Expression
  ): Seq[NamedExpression]

  /**
   * Utility method to assemble a list of NamedAttributes for the specified encoding.  
   * 
   * @param oldPlan       The pre-annotation plan (used to get [[attributes]] if omitted)
   * @param newPlan       The plan being annotated
   * @param attributes    An explicit list of attributes to build annotations for (optional, will 
   *                      use [[default]] for all attributes in oldPlan.output if omitted)
   * @param default       The default annotation.  (optional, defaults to the existing annotation in 
   *                      [[newPlan]])
   * @param replace       A list of attribute annotations to replace.  Ignored if [[attributes]] is 
   *                      also provided.  (optional)
   * @param row           A replacement row annotation.  (optional, defaults to [[default]] if not 
   *                      rovided).
   * @param addToRow      A supplemental row annotation.  Will be ORred with [[row]].
   * 
   * This function is designed both to handle explicit overrides for the annotations (the 
   * [[attributes]] and [[row]] parameters), as well as to support deltas (the [[replace]] and 
   * [[addToRow]] parameters).
   */
  def annotations(
    oldPlan: LogicalPlan,
    newChild: LogicalPlan,
    attributes: Seq[(Attribute, Expression)] = null,
    default: Expression = null,
    replace: Seq[(Attribute, Expression)] = Seq(),
    row: Expression = null,
    addToRow: Seq[Expression] = Seq()
  ): Seq[NamedExpression] = 
  {
    val getDefault = Option(default).map { e => {(x:Attribute) => e} }
                                    .getOrElse { annotationFor(_) }
    getAnnotationExpressions(
      oldPlan, 
      newChild, 
      (
        if(attributes == null){
          val byExprId = replace.map { case (a, v) => a.exprId -> v }
                                .toMap
          oldPlan.output.map { attr => 
            attr -> byExprId.getOrElse(attr.exprId, getDefault(attr))
          }
        } else { attributes } 
      ),
      foldOr((
        (
          if(row != null){ row }
          else if(default != null) { default }
          else { annotationForRow }
        ) +: addToRow
      ):_*)
    )
  }

  /**
   * Annotate the specified plan.  See [[annotations]] for details on parameter usage.
   */
  def annotate(
    oldPlan: LogicalPlan,
    newPlan: LogicalPlan,
    attributes: Seq[(Attribute, Expression)] = null,
    default: Expression = null,
    replace: Seq[(Attribute, Expression)] = Seq(),
    row: Expression = null,
    addToRow: Seq[Expression] = Seq()
  ): LogicalPlan = 
    Project(
      oldPlan.output ++ annotations(oldPlan, newPlan, attributes, default, replace, row, addToRow),
      newPlan
    )

  def join(
    oldPlan: LogicalPlan,
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    build: (LogicalPlan, LogicalPlan) => LogicalPlan,
    addToRow: Seq[Expression] = Seq()
  ): LogicalPlan = 
  {
    // print(s"--- old ----\n$oldPlan\n--- lhs ----\n$lhs\n--- rhs ----\n$rhs\n----------\n")
    val rhsRowAttr = AttributeReference("__MIMIR_JOIN_RHS_TEMP", annotationForRow.dataType)()
    val rhsNonRowAttrs = 
      rhs.output.filterNot { _.name.equals(annotationForRow.name) } 
    val safeRhs = 
      Project(
        rhsNonRowAttrs :+ Alias(annotationForRow, rhsRowAttr.name)(rhsRowAttr.exprId),
        rhs
      )

    annotate(
      oldPlan, 
      build(lhs, safeRhs),
      addToRow = rhsRowAttr +: addToRow
    )
  }
}