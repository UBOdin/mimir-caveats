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

object CaveatExistsBooleanAttributeEncoding
{

  def attributeAnnotationExpression(
    attr: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    UnresolvedAttribute(attributeAnnotationName(attr, annotation))

  def attributeAnnotationName(
    attr: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ) = s"${annotation}_ATTRIBUTE_${attr}"

  def rowAnnotationExpression(
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    UnresolvedAttribute(rowAnnotationName(annotation))

  def rowAnnotationName(
    annotation: String = ANNOTATION_ATTRIBUTE
  ) = s"${annotation}_ROW" 
}
