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
    *  Schema of the caveat columns for annotation prefix _P is:
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
  def annotationStruct(baseSchema:StructType, prefix: String = ANNOTATION_ATTRIBUTE): StructType = {
    StructType(rowAnnotationStruct(prefix) ++ attributeAnnotationStruct(baseSchema, prefix))
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
  def rowAnnotationExpressions(annotation: String = ANNOTATION_ATTRIBUTE): Seq[Expression] =
    Seq(
      UnresolvedAttribute(addAnnotPrefix(ROW_FIELD, LOWER_BOUND_FIELD, annotation)),
      UnresolvedAttribute(addAnnotPrefix(ROW_FIELD, BEST_GUESS_FIELD, annotation)),
      UnresolvedAttribute(addAnnotPrefix(ROW_FIELD, UPPER_BOUND_FIELD, annotation))
    )


  // get all attribute annotations
  def allAttributeAnnotationsExpressions(baseSchema: StructType, annotation: String = ANNOTATION_ATTRIBUTE): Seq[Expression] =
    baseSchema.fields.flatMap{ x =>
      Seq(
        UnresolvedAttribute(addAnnotPrefix(x.name, LOWER_BOUND_FIELD, annotation)),
        UnresolvedAttribute(addAnnotPrefix(x.name, UPPER_BOUND_FIELD, annotation))
      )
    }

  // get access to annotation of an individual attribute
  def attributeAnnotationExpressions(
    attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Seq[Expression] =
    Seq(
      UnresolvedAttribute(addAnnotPrefix(attrName, LOWER_BOUND_FIELD, annotation)),
      UnresolvedAttribute(addAnnotPrefix(attrName, UPPER_BOUND_FIELD, annotation))
    )

  def rowLBexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    lbExpression(rowAnnotationExpressions(annotation))

  def rowBGexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    bgRowExpression(rowAnnotationExpression(annotation))

  def rowUBexpression(annotation: String = ANNOTATION_ATTRIBUTE): Expression =
    ubExpression(rowAnnotationExpression(annotation))

  def rowAnnotationExpressionTriple(annotation: String = ANNOTATION_ATTRIBUTE): RangeBoundedExpr =
    RangeBoundedExpr(
      rowLBexpression(annotation),
      rowBGexpression(annotation),
      rowUBexpression(annotation)
    )


  def attrLBexpression(attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    lbExpression(
      UnresolvedExtractValue(
        allAttributeAnnotationsExpression(annotation),
        Literal(attrName)
      )
    )

  def attrUBexpression(attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    ubExpression(
      UnresolvedExtractValue(
        allAttributeAnnotationsExpression(annotation),
        Literal(attrName)
      )
    )

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

  def addAnnotPrefix(attr: String, suffix: String, prefix: String = ANNOTATION_ATTRIBUTE) = prefix + "_" + attr + "_" + suffix

}
