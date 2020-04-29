package org.mimirdb.caveats.annotate


import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  NamedExpression,
  AttributeReference,
  Attribute,
  ExprId,
  Alias
}
import org.apache.spark.sql.types.BooleanType

object CaveatExistsAttributeAnnotation
  extends IntermediateEncoding
{
  val annotationForRow = AttributeReference("__MIMIR_ROW_TAINT", BooleanType)()
  val attributeAnnotations = scala.collection.mutable.Map[ExprId, Attribute]()
  def annotationFor(attr: Attribute) = 
  {
    if(!attributeAnnotations.contains(attr.exprId)){
      attributeAnnotations.put(attr.exprId, AttributeReference(s"__MIMIR_ATTRIBUTE_${attr.exprId.id}_${attr.name}", BooleanType)())
    }
    attributeAnnotations(attr.exprId)
  }

  def aliasTo(a:Attribute, e:Expression) = 
    Alias(e, a.name)(a.exprId)
    
  def getAnnotationExpressions(
    oldPlan: LogicalPlan,
    newPlan: LogicalPlan,
    attributes: Seq[(Attribute, Expression)],
    row: Expression
  ): Seq[NamedExpression] =
    attributes.map { case (a, e) => aliasTo(a, e) } :+ aliasTo(annotationForRow, row)
  
}