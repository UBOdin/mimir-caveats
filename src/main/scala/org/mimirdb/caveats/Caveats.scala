package org.mimirdb.caveats

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

object Caveats
{


  /**
   * Extend the provided [DataFrame] with an annotation attribute.
   * 
   * The attribute will use the identifier [Caveats.ANNOTATION_ATTRIBUTE].  It 
   * will be a [Struct] with two fields identified by [Caveat.ROW_FIELD] and 
   * [Caveat.ATTRIBUTE_FIELD].  The row annotation is Boolean-typed, while the
   * attribute annotation is a structure with one Boolean-typed field for each 
   * attribute of the input [DataFrame] (i.e. `df.output`).

   * @param   dataset           The [DataFrame] to anotate
   * @param   pedantic          If true, everything is annotated according to 
   *                            the official spec.  This may reduce performance
   *                            or overwhelm the results with too many 
   *                            annotations.
   * @param   ignoreUnsupported If true, attempt to work around unsupported plan
   *                            operators.  We make no guarantees about the 
   *                            correctness of the resulting annotations.
   * @return                    [dataset] extended with an annotation attribute
   **/
  def annotate(dataset:DataFrame, 
    style: AnnotationStyle = CaveatExists,
    pedantic: Boolean = true,
    ignoreUnsupported: Boolean = false,
    trace: Boolean = false): DataFrame = 
  {
    val execState = dataset.queryExecution
    val plan = execState.analyzed
    val annotated = style(
        pedantic = pedantic, 
        ignoreUnsupported = ignoreUnsupported,
        trace = trace
      )(plan)
    val baseSchema = plan.schema

    return new DataFrame(
      execState.sparkSession,
      annotated,
      RowEncoder(
        plan.schema.add(
          ANNOTATION_ATTRIBUTE, 
          annotationStruct(plan.schema.fieldNames),
          false
        )
      )
      ///RowEncoder() 
      // ^---- UUUUGLY.  We should really be using dataset.encoder, but it's PRIVATE!!!!
      //       (and final, so we can't make it accessible with reflection)
    )
  }


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

  def annotationStruct(fieldNames:Seq[String]): StructType =
  {
    StructType(Seq(
      StructField(ROW_FIELD, BooleanType, false),
      StructField(ATTRIBUTE_FIELD, StructType(
        fieldNames.map { 
          StructField(_, BooleanType, false)
        }
      ), false)
    ))
  }
}
