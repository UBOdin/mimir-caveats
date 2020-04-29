package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Literal,
  Attribute
}

import org.mimirdb.caveats.UncertaintyModel
import org.mimirdb.caveats.Constants._
import org.apache.spark.sql.types.StringType

/*
 * Instrument a logical plan to propagate a specific type of annotations.
 */
trait AnnotationInstrumentationStrategy
{
  // rewrite a logical plan to propagate annotations
  def apply(plan: LogicalPlan): LogicalPlan

  // return annotation encoding produced by this instrumentation style
  def outputEncoding: AnnotationEncoding

  // return the type of annotations that are propagated
  def annotationType: AnnotationType

  // translates from a particular type of uncertainty model into this type of annotation
  def translateFromUncertaintyModel(plan: LogicalPlan, model: UncertaintyModel): LogicalPlan = {
    throw new AnnotationException(s"translation from $model not supported")
  }
}

/*
 * A type of annotation. This is a logical type. For each logical type there may exist one or more encodings that store such annotations.
 */
trait AnnotationType
{
  def defaultEncoding: AnnotationEncoding
}

/*
 * An encoding of a type of annotation `AnnotationType`
 *  boolean row level
 *  boolean row level + boolean attribute level
 *  semiring N^3 annotation + range annotations for attributes
 *
 *  the assumption is that annotation are stored in a single attribute (using nested types)
 */
trait AnnotationEncoding
{
  // return struct type used to store annotations of this schema
  def annotationStruct(fieldNames:Seq[String]): StructType = {
    annotationStruct(StructType(fieldNames.map { f => StructField(f, StringType, true) }))
  }

  // return struct type used to store annotations for this schema
  def annotationStruct(baseSchema:StructType): StructType

  // get expression to access row annotation
  def rowAnnotationExpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression

  // get all attribute annotations
  def allAttributeAnnotationsExpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression

  // get access to annotation of an individual attribute
  def attributeAnnotationExpression(
    attr: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression

  def attributeAnnotationExpressionFromAttr(
    attr: Attribute,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    attributeAnnotationExpression(attr.name, annotation)
}
