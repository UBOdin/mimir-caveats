package org.mimirdb.caveats.annotate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedExtractValue,
  UnresolvedAttribute
}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Literal,
  Attribute
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ StructType, StructField, BooleanType }

import org.mimirdb.caveats.annotate._
import org.mimirdb.caveats.Constants._


import org.mimirdb.caveats.Constants._

object CaveatExistsBooleanStructEncoding extends AnnotationEncoding
{

  def allAttributeAnnotationsExpression(
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(annotation),
      Literal(ATTRIBUTE_FIELD)
    )

  def attributeAnnotationExpression(
    attr: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    UnresolvedExtractValue(
      allAttributeAnnotationsExpression(annotation),
      Literal(attr)
    )

  def rowAnnotationExpression(
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(annotation),
      Literal(ROW_FIELD)
    )

  def annotationStruct(baseSchema:StructType): StructType =
  {
    StructType(Seq(
      StructField(ROW_FIELD, BooleanType, false),
      StructField(ATTRIBUTE_FIELD, StructType(
        baseSchema.map { a =>
          StructField(a.name, BooleanType, false)
        }
      ), false)
    ))
  }

}
