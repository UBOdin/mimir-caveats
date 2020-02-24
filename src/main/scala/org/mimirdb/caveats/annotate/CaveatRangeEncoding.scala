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
    *  Schema of the caveat column is:
    *  ROW_FIELD
    *      LOWER_BOUND_FIELD
    *      BEST_GUESS_FIELD
    *      UPPER_BOUND_FIELD
    *  ATTRIBUTE_FIELD
    *      A1
    *           LOWER_BOUND_FIELD
    *           UPPER_BOUND_FIELD
    *      ...
    *      An
    *           LOWER_BOUND_FIELD
    *           UPPER_BOUND_FIELD
    */
  def annotationStruct(baseSchema:StructType): StructType = {
    StructType(Seq(
      StructField(ROW_FIELD,
        StructType(Seq(
          StructField(LOWER_BOUND_FIELD,IntegerType, false),
          StructField(BEST_GUESS_FIELD,IntegerType, false),
          StructField(UPPER_BOUND_FIELD,IntegerType, false)
        )), false),
      StructField(ATTRIBUTE_FIELD, StructType(
        baseSchema.fields.map { x =>
          StructField(x.name,
            StructType(Seq(
              StructField(LOWER_BOUND_FIELD, x.dataType, false),
              StructField(UPPER_BOUND_FIELD, x.dataType, false)
            ))
          )
        }
      ), false)
    ))
  }

  // get expression to access row annotation
  def rowAnnotationExpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(annotation),
      Literal(ROW_FIELD)
    )


  // get all attribute annotations
  def allAttributeAnnotationsExpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(annotation),
      Literal(ATTRIBUTE_FIELD),
    )

  // get access to annotation of an individual attribute
  def attributeAnnotationExpression(
    attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    UnresolvedExtractValue(
      allAttributeAnnotationsExpression(annotation),
      Literal(attrName)
    )

  def rowLBexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    lbExpression(rowAnnotationExpression(annotation))

  def rowBGexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    bgRowExpression(rowAnnotationExpression(annotation))

  def rowUBexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    ubExpression(rowAnnotationExpression(annotation))


  def lbExpression(e: Expression): Expression =
    UnresolvedExtractValue(
      e,
      Literal(LOWER_BOUND_FIELD)
    )

  def bgAttrExpression(a: Attribute): Expression =
    a //TODO do we want this to work like this?

  def bgRowExpression(e: Expression): Expression =
    UnresolvedExtractValue(
      e,
      Literal(BEST_GUESS_FIELD)
    )

  def ubExpression(e: Expression): Expression =
    UnresolvedExtractValue(
      e,
      Literal(UPPER_BOUND_FIELD)
    )



}
