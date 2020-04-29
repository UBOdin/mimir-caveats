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
    val rhsRowAttr = AttributeReference("__MIMIR_JOIN_RHS_TEMP", annotationForRow.dataType)()
    val rhsNonRowAttrs = 
      rhs.output.filter { _.exprId.equals(annotationForRow.exprId) } 
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