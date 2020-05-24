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
  Attribute,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, BooleanType }

import org.mimirdb.caveats.annotate._
import org.mimirdb.caveats.Constants._

/**
  * Encoding of [CaveatRangeType] annotation for which we record for each attribute
  * value an lower and upper bound across all possible worlds.
  * Furthermore, for each row we record and upper / lower bound on its
  * annotation (multiplicity) across all worlds.
  *
  *  Currently only top-level attributes are annotated. So this would not work
  *  for attributes that are nested.
  **/
object CaveatRangeEncoding
  extends AnnotationEncoding
{

  /**
    * Schema of the caveat columns for an input schema (A1, ..., An) with
    * annotation prefix [ANNOT_PREFIX] is:
    *
    * [ANNOT_PREFIX]_ROW_LOWER_BOUND
    * [ANNOT_PREFIX]_ROW_BEST_GUESS
    * [ANNOT_PREFIX]_ROW_UPPER_BOUND
    * [ANNOT_PREFIX]_A1_LOWER_BOUND
    * [ANNOT_PREFIX]_A1_UPPER_BOUND
    * ...
    * [ANNOT_PREFIX]_An_LOWER_BOUND
    * [ANNOT_PREFIX]_An_UPPER_BOUND
    */
  def annotationStruct(baseSchema:StructType, prefix: String = ANNOTATION_ATTRIBUTE): StructType = {
    StructType(rowAnnotationStruct(prefix) ++ attributeAnnotationStruct(baseSchema, prefix))
  }

  override def isValidAnnotatedStructTypeSchema(schema: StructType, prefix: String = ANNOTATION_ATTRIBUTE): Boolean = {
    val normalAttrs = StructType(getNormalAttributes(schema, prefix))
    val annotatedSchema = annotationStruct(normalAttrs, prefix)
    schema == annotatedSchema
  }

  def isValidAnnotatedSchema(schema: Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): Boolean = {
    val normalAttrs = StructType(getNormalAttributes(schema, prefix)
      .map(x => StructField(x, IntegerType, false)))
    val annotatedSchema = annotationStruct(normalAttrs, prefix)
    schema == annotatedSchema
  }

  def getNormalAttributes(schema: Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): Seq[String] = {
    schema.filter(x => x.startsWith(prefix))
  }

  def rowAnnotationStruct(prefix:String) : Seq[StructField] =
    Seq(
      StructField(addAnnotPrefix(ROW_FIELD, LOWER_BOUND_FIELD ,prefix), IntegerType, false),
      StructField(addAnnotPrefix(ROW_FIELD, BEST_GUESS_FIELD, prefix), IntegerType, false),
      StructField(addAnnotPrefix(ROW_FIELD, UPPER_BOUND_FIELD, prefix), IntegerType, false)
    )

  def attributeAnnotationStruct(baseSchema: StructType, prefix: String): Seq[StructField] =
  {
    baseSchema.fields.flatMap { x =>
      Seq(
        StructField(addAnnotPrefix(x.name, LOWER_BOUND_FIELD, prefix), x.dataType, false),
        StructField(addAnnotPrefix(x.name, UPPER_BOUND_FIELD, prefix), x.dataType, false)
      )
    }
  }

  // get expression to access row annotation
  def rowAnnotationExpressions(annotation: String = ANNOTATION_ATTRIBUTE): Seq[NamedExpression] =
    Seq(
      UnresolvedAttribute(addAnnotPrefix(ROW_FIELD, LOWER_BOUND_FIELD, annotation)),
      UnresolvedAttribute(addAnnotPrefix(ROW_FIELD, BEST_GUESS_FIELD, annotation)),
      UnresolvedAttribute(addAnnotPrefix(ROW_FIELD, UPPER_BOUND_FIELD, annotation))
    )


  // get all attribute annotations
  def allAttributeAnnotationsExpressions(baseSchema: StructType, annotation: String = ANNOTATION_ATTRIBUTE): Seq[NamedExpression] =
    baseSchema.fields.flatMap{ x =>
      Seq(
        UnresolvedAttribute(addAnnotPrefix(x.name, LOWER_BOUND_FIELD, annotation)),
        UnresolvedAttribute(addAnnotPrefix(x.name, UPPER_BOUND_FIELD, annotation))
      )
    }

  override def allAttributeAnnotationsExpressionsFromExpressions(baseSchema: Seq[NamedExpression], prefix: String = ANNOTATION_ATTRIBUTE): Seq[NamedExpression] = {
    allAttributeAnnotationsExpressions(StructType(baseSchema.map(x => StructField(x.name, x.dataType))), prefix)
  }

  // get access to annotation of an individual attribute
  def attributeAnnotationExpressions(
    attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Seq[NamedExpression] =
    Seq(
      UnresolvedAttribute(addAnnotPrefix(attrName, LOWER_BOUND_FIELD, annotation)),
      UnresolvedAttribute(addAnnotPrefix(attrName, UPPER_BOUND_FIELD, annotation))
    )

  def attributeAnnotationExpressionFromAttr(
    a: Attribute,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Seq[NamedExpression] =
    attributeAnnotationExpressions(a.name, annotation)

  def rowLBexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    lbExpression(rowAnnotationExpressions(annotation))

  def rowBGexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    bgRowExpression(rowAnnotationExpressions(annotation))

  def rowUBexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    ubExpression(rowAnnotationExpressions(annotation))

  def rowAnnotationExpressionTriple(annotation: String = ANNOTATION_ATTRIBUTE): RangeBoundedExpr =
    RangeBoundedExpr(
      rowLBexpression(annotation),
      rowBGexpression(annotation),
      rowUBexpression(annotation)
    )

  def attrLBexpression(attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    attributeAnnotationExpressions(attrName, annotation)(0)

  def bgAttrExpression(a: Attribute): Expression =
    a //TODO do we want this to work like this?

  def attrUBexpression(attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    attributeAnnotationExpressions(attrName, annotation)(1)

  def lbExpression(boundedExpr: Seq[Expression]): Expression =
    boundedExpr(0)

  def bgRowExpression(boundedExpr: Seq[Expression]): Expression =
    boundedExpr(1)

  def ubExpression(boundedExpr: Seq[Expression]): Expression =
    boundedExpr(2)

  def addAnnotPrefix(attr: String, suffix: String, prefix: String = ANNOTATION_ATTRIBUTE) = prefix + "_" + attr + "_" + suffix

}
