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


trait IntermediateEncodingDescription
{
  def annotationForRow: Expression
  def annotationFor(attr: Attribute): Expression
  def allAnnotations: Seq[(Attribute, Expression)]
}

trait IntermediateEncoding[D <: IntermediateEncodingDescription]
{

  /**
   * Assemble an instance of the specified intermediate encoding, 
   * allocating fresh names for annotated attributes
   */
  def build(
    attributes: Seq[(Attribute, Expression)],
    row: Expression
  ): (Seq[NamedExpression], D)


  def merge(elements: Seq[D]): (Seq[NamedExpression], D) =
    build(
      elements.flatMap { _.allAnnotations },
      foldOr( elements.map { _.annotationForRow }:_* )
    )

  // def annotationForRow: NamedExpression
  // def annotationFor(attr: Attribute): NamedExpression

  // def getAnnotationExpressions(
  //   oldPlan: LogicalPlan,
  //   newChild: LogicalPlan,
  //   attributes: Seq[(Attribute, Expression)],
  //   row: Expression
  // ): Seq[NamedExpression]

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
    newPlanDescription: D,
    attributes: Seq[(Attribute, Expression)] = null,
    default: Expression = null,
    replace: Seq[(Attribute, Expression)] = Seq(),
    row: Expression = null,
    addToRow: Seq[Expression] = Seq()
  ): (Seq[NamedExpression], D) = 
  {
    val getDefault = Option(default).map { e => {(x:Attribute) => e} }
                                    .getOrElse { newPlanDescription.annotationFor(_) }
    build(
      (
        if(attributes == null){
          val byExprId = replace.map { case (a, v) => a.exprId -> v }
                                .toMap
          oldPlan.output
                 .map { case attr => 
                   attr -> byExprId.getOrElse(attr.exprId, getDefault(attr))
                 }
        } else { attributes } 
      ),
      foldOr((
        (
          if(row != null){ row }
          else if(default != null) { default }
          else { newPlanDescription.annotationForRow }
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
    newPlanDescription: D,
    attributes: Seq[(Attribute, Expression)] = null,
    default: Expression = null,
    replace: Seq[(Attribute, Expression)] = Seq(),
    row: Expression = null,
    addToRow: Seq[Expression] = Seq()
  ): (LogicalPlan, D) = 
  {
    val (annotationExpressions, description): (Seq[NamedExpression], D) =
        annotations(
          oldPlan = oldPlan, 
          newPlanDescription = newPlanDescription,
          attributes = attributes, 
          default = default, 
          replace = replace, 
          row = row, 
          addToRow = addToRow
        )
    (
      Project(
        oldPlan.output ++ annotationExpressions,
        newPlan
      ),
      description
    )
  }
}