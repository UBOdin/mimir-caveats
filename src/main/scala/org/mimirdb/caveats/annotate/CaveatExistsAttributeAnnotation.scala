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

class CaveatExistsAttributeAnnotation(
  val annotationForRow: AttributeReference,
  val annotationsForAttributes: Map[ExprId, AttributeReference],
  val allAttributes: Seq[Attribute]
)
  extends IntermediateEncodingDescription
{
  def annotationFor(attr: Attribute): Expression = 
    annotationsForAttributes
      .get(attr.exprId)
      .getOrElse { 
        throw new NoSuchElementException(
          s"Missing annotation for $attr (out of ${allAttributes.mkString(", ") })"
        )
      }

  def allAnnotations =
    allAttributes.map { attr => 
      attr -> annotationFor(attr)
    }

  override def toString(): String = 
  {
    "{ " + 
      allAttributes.map { a => a -> annotationsForAttributes(a.exprId) }
                   .toMap
                   .mkString(", ") + 
    " } /// " + annotationForRow
  }
}

object CaveatExistsAttributeAnnotation
  extends IntermediateEncoding[CaveatExistsAttributeAnnotation]
{
  type description = CaveatExistsAttributeAnnotation

  val ROW_ANNOTATION = "__MIMIR_ROW_CAVEATTED"
  def ATTR_ANNOTATION(attr: Attribute) = 
    s"__MIMIR_ATTR_CAVEATTED__${attr.name}_${attr.exprId.id}"

  def build(
    attributes: Seq[(Attribute, Expression)],
    row: Expression
  ): (Seq[NamedExpression], CaveatExistsAttributeAnnotation) =
  {
    val rowExpression = Alias(row, ROW_ANNOTATION)()
    val attrExpressions = attributes.map { case (attr, expr) =>
                            attr -> Alias(expr, ATTR_ANNOTATION(attr))()
                          }.toIndexedSeq

    (
      attrExpressions.map { _._2 } :+ rowExpression,
      new CaveatExistsAttributeAnnotation(
        annotationForRow = 
          AttributeReference(
            rowExpression.name, 
            BooleanType,
            false,
          )(exprId = rowExpression.exprId),
        annotationsForAttributes = 
          attrExpressions.map { case (attr, attrExpression) => 
            attr.exprId -> 
              AttributeReference(
                attrExpression.name, 
                BooleanType,
                false,
              )(exprId = attrExpression.exprId)
          }.toMap,
          attrExpressions.map { _._1 }
      )
    )
  }
}
