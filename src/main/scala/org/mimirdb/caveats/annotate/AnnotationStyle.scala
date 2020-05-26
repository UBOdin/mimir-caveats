package org.mimirdb.caveats.annotate

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Literal,
  Attribute,
  NamedExpression
}

import org.mimirdb.caveats.UncertaintyModel
import org.mimirdb.caveats.Constants._
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.attribute.UnresolvedAttribute



/*
 * Instrument a logical plan to propagate a specific type of annotations.
 */
trait AnnotationInstrumentationStrategy
{
  // rewrite a logical plan to propagate annotations
  def apply(plan: LogicalPlan, trace: Boolean = false): LogicalPlan

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

/**
 * An encoding of a type of annotation `AnnotationType`
 *  boolean row level
 *  boolean row level + boolean attribute level
 *  semiring N^3 annotation + range annotations for attributes
 */
trait AnnotationEncoding
{
  // return the annotated schema for this basic schema
  def annotatedSchema(baseSchema:StructType, prefix: String = ANNOTATION_ATTRIBUTE): StructType = {
    StructType(baseSchema.fields ++ annotationStruct(baseSchema, prefix).fields)
  }

  // return struct type used to store annotations for this schema
  def annotationStruct(baseSchema:StructType, prefix: String = ANNOTATION_ATTRIBUTE): StructType

  // return struct type used to store annotations of this schema
  def annotationStructFromAttrs(fieldNames:Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): StructType = {
    annotationStruct(StructType(fieldNames.map { f => StructField(f, StringType, true) }), prefix)
  }

  // given a schema returns true if the schema has the correct annotation attributes
  def isValidAnnotatedStructTypeSchema(schema: StructType, prefix: String  = ANNOTATION_ATTRIBUTE): Boolean = {
    isValidAnnotatedSchema(schema.fields.map(x => x.name), prefix)
  }

  def isValidAnnotatedNamedExpressionSchema(schema: Seq[NamedExpression], prefix: String = ANNOTATION_ATTRIBUTE): Boolean = {
    isValidAnnotatedSchema(schema.map(x => x.name).toSeq, prefix)
  }

  def isValidAnnotatedSchema(schema: Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): Boolean

  // given an annotated schema return the non-annotation attributes
  def getNormalAttributesFromSparkType(schema: StructType, prefix: String): Seq[StructField] = {
    val annotNames = getNormalAttributes(schema.fields.map(x => x.name), prefix)
    StructType(schema.fields.filter(x => annotNames.contains(x.name)))
  }

  def getNormalAttributesFromNamedExpressions(schema: Seq[NamedExpression], prefix: String = ANNOTATION_ATTRIBUTE): Seq[NamedExpression] = {
    //println(s"in schema $schema")
    val normalAttrs = getNormalAttributes(schema.map(x => x.name), prefix)
    //println(s"annot atttributes $normalAttrs")
    val normal = schema.filter(x => normalAttrs.contains(x.name))
    //println(s"normal: $normal")
    normal
  }

  def getNormalAttributes(schema: Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): Seq[String]

  // return schema after adding annotations
  def annotatedSchema(baseSchema:StructType): StructType = {
    StructType(baseSchema.fields ++ annotationStruct(baseSchema).fields)
  }

  // get expression to access row annotation
  def rowAnnotationExpressions(prefix: String = ANNOTATION_ATTRIBUTE): Seq[Expression]

  // get all attribute annotations
  def allAttributeAnnotationsExpressions(baseSchema: StructType, prefix: String = ANNOTATION_ATTRIBUTE): Seq[Expression]

  def allAttributeAnnotationsExpressionsFromExpressions(baseSchema: Seq[NamedExpression], prefix: String = ANNOTATION_ATTRIBUTE): Seq[Expression] = {
    allAttributeAnnotationsExpressions(StructType(baseSchema.map(x => StructField(x.name, x.dataType))), prefix)
  }

  // get access to annotation of an individual attribute
  def attributeAnnotationExpressions(
    attr: String,
    prefix: String = ANNOTATION_ATTRIBUTE
  ): Seq[Expression]

  def attributeAnnotationExpressionFromAttrs(
    attr: Attribute,
    prefix: String = ANNOTATION_ATTRIBUTE
  ): Seq[Expression] =
    attributeAnnotationExpressions(attr.name, prefix)
}

/**
  * A special type of [AnnotationEncoding] that uses a single attribute to store annotations.
  */
trait SingleAttributeAnnotationEncoding extends AnnotationEncoding {

  def annotationStruct(baseSchema:StructType, prefix: String = ANNOTATION_ATTRIBUTE): StructType = {
    StructType(Seq(StructField(prefix, nestedAnnotationAttributeStruct(baseSchema), false)))
  }

  def nestedAnnotationAttributeStruct(baseSchema:StructType): StructType

  def rowAnnotationExpressions(prefix: String = ANNOTATION_ATTRIBUTE): Seq[Expression] = {
    Seq(rowAnnotationExpression(prefix))
  }

  def rowAnnotationExpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression

  def allAttributeAnnotationsExpressions(baseSchema: StructType, prefix: String = ANNOTATION_ATTRIBUTE): Seq[Expression] = {
    Seq(allAttributeAnnotationsExpression(prefix))
  }

  def allAttributeAnnotationsExpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression

  def attributeAnnotationExpressions(
    attr: String,
    prefix: String = ANNOTATION_ATTRIBUTE
  ): Seq[Expression] = {
    Seq(attributeAnnotationExpression(attr,prefix))
  }

  def attributeAnnotationExpression(attr: String, annotation: String = ANNOTATION_ATTRIBUTE): Expression

  def getNormalAttributes(schema: Seq[String], prefix: String): Seq[String] = schema.filterNot(_ == prefix)

}
