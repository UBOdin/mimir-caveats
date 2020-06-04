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
    val normalAttrs = StructType(getNormalAttributesFromSparkType(schema, prefix))
    val annotatedSchema = annotationStruct(normalAttrs, prefix)
//        println(s"================================================================================\ncheck schema is valid whether valid:\n\nnormal attributes: $normalAttrs\nannotated schema: $annotatedSchema\ncheck schema: $schema")
    schema.fields.toSeq == normalAttrs ++ annotatedSchema.fields
  }

  def isValidAnnotatedSchema(schema: Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): Boolean = {
    val normalAttrs = StructType(getNormalAttributes(schema, prefix)
      .map(x => StructField(x, IntegerType, false)))
    val annotatedSchema = annotationStruct(normalAttrs, prefix)
    schema == normalAttrs.map( _.name) ++ annotatedSchema.fields.map( _.name)
  }

  def getNormalAttributes(schema: Seq[String], prefix: String = ANNOTATION_ATTRIBUTE): Seq[String] = {
    schema.filterNot(x => x.startsWith(prefix))
  }

  def rowAnnotationStruct(prefix:String) : Seq[StructField] =
    Seq(
      StructField(addAnnotPrefix(ROW_FIELD, LOWER_BOUND_FIELD, prefix), IntegerType, false),
      StructField(addAnnotPrefix(ROW_FIELD, BEST_GUESS_FIELD, prefix), IntegerType, false),
      StructField(addAnnotPrefix(ROW_FIELD, UPPER_BOUND_FIELD, prefix), IntegerType, false)
    )

  def rowAnnotationAttrNames(prefix: String = ANNOTATION_ATTRIBUTE): Seq[String] = Seq (
    addAnnotPrefix(ROW_FIELD, LOWER_BOUND_FIELD, prefix),
    addAnnotPrefix(ROW_FIELD, BEST_GUESS_FIELD, prefix),
    addAnnotPrefix(ROW_FIELD, UPPER_BOUND_FIELD, prefix)
  )

  def attributeAnnotationStruct(baseSchema: StructType, prefix: String): Seq[StructField] =
  {
    baseSchema.fields.flatMap { x =>
      Seq(
        StructField(addAnnotPrefix(x.name, LOWER_BOUND_FIELD, prefix), x.dataType, true),
        StructField(addAnnotPrefix(x.name, UPPER_BOUND_FIELD, prefix), x.dataType, true)
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

  // check whether an attribute is an annotaiton attribute
  def isAnnotationAttribute(name: String, annotation: String = ANNOTATION_ATTRIBUTE): Boolean =
    name.startsWith(annotation)

  def isRowAnnotationAttribute(name: String, annotation: String = ANNOTATION_ATTRIBUTE): Boolean = name.startsWith(addAnnotPrefix(ROW_FIELD, "", annotation))

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

  def attributeAnnotationAttrName(
    attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Seq[String] = Seq(
    addAnnotPrefix(attrName, LOWER_BOUND_FIELD, annotation),
    addAnnotPrefix(attrName, UPPER_BOUND_FIELD, annotation)
  )

  def attributeAnnotationExpressionFromAttr(
    a: Attribute,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Seq[NamedExpression] =
    attributeAnnotationExpressions(a.name, annotation)

  def rowLBexpression(annotation: String = ANNOTATION_ATTRIBUTE): NamedExpression =
    lbExpression(rowAnnotationExpressions(annotation))

  def rowBGexpression(annotation: String = ANNOTATION_ATTRIBUTE): NamedExpression =
    bgRowExpression(rowAnnotationExpressions(annotation))

  def rowUBexpression(annotation: String = ANNOTATION_ATTRIBUTE): NamedExpression =
    ubExpression(rowAnnotationExpressions(annotation))

  def rowAnnotationExpressionTriple(annotation: String = ANNOTATION_ATTRIBUTE): RangeBoundedExpr[NamedExpression] =
    RangeBoundedExpr(
      rowLBexpression(annotation),
      rowBGexpression(annotation),
      rowUBexpression(annotation)
    )

  def attrLBexpression(attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    attributeAnnotationExpressions(attrName, annotation)(0)

  def attrBGexpression(a: Attribute): Expression =
    a //TODO do we want this to work like this?

  def attrUBexpression(attrName: String,
    annotation: String = ANNOTATION_ATTRIBUTE
  ): Expression =
    attributeAnnotationExpressions(attrName, annotation)(1)

  def lbExpression(boundedExpr: Seq[NamedExpression]): NamedExpression =
    boundedExpr(0)

  def bgRowExpression(boundedExpr: Seq[NamedExpression]): NamedExpression =
    boundedExpr(1)

  def ubExpression(boundedExpr: Seq[NamedExpression]): NamedExpression =
    boundedExpr(2)

  def addAnnotPrefix(attr: String, suffix: String, prefix: String = ANNOTATION_ATTRIBUTE) = prefix + "_" + attr + "_" + suffix

}


/**
  * Encoding of [CaveatRangeType] annotation for which we record for each attribute
  * value an lower and upper bound across all possible worlds.
  * Furthermore, for each row we record and upper / lower bound on its
  * annotation (multiplicity) across all worlds.
  *
  *  Currently only top-level attributes are annotated. So this would not work
  *  for attributes that are nested.
  **/
object SingleAttributeCaveatRangeEncoding
  extends SingleAttributeAnnotationEncoding
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
  def nestedAnnotationAttributeStruct(baseSchema:StructType): StructType = {
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

  // Members declared in org.mimirdb.caveats.annotate.AnnotationEncoding
  def isValidAnnotatedSchema(schema: Seq[String],prefix: String): Boolean = ???


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

  def rowAnnotationExpressionTriple(annotation: String = ANNOTATION_ATTRIBUTE): RangeBoundedExpr[Expression] =
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



}
