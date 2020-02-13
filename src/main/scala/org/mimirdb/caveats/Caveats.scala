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

object Caveats
{

  val ANNOTATION_COLUMN  = "__CAVEATS"
  val ROW_ANNOTATION     = "ROW"
  val COLUMN_ANNOTATION  = "COLUMN"


  /**
   * Extend the provided [DataFrame] with an annotation column.
   * 
   * The column will use the identifier [Caveats.ANNOTATION_COLUMN].  It will be
   * a [Struct] with two fields identified by [Caveat.ROW_ANNOTATION] and 
   * [Caveat.COLUMN_ANNOTATION].  The row annotation is Boolean-typed, while the
   * column annotation is a structure with one Boolean-typed field for each 
   * column of the input [DataFrame].

   * @param   dataset  The [DataFrame] to anotate
   * @return           [dataset] extended with an annotation column
   **/
  def annotate(dataset:DataFrame): DataFrame = 
  {
    val execState = dataset.queryExecution
    val plan = execState.analyzed
    val annotated = annotate(plan)
    val baseSchema = plan.schema

    return new DataFrame(
      execState.sparkSession,
      annotated,
      RowEncoder(
        plan.schema.add(
          ANNOTATION_COLUMN, 
          annotationStruct(plan.schema),
          false
        )
      )
      ///RowEncoder() 
      // ^---- UUUUGLY.  We should really be using dataset.encoder, but it's PRIVATE!!!!
      //       (and final, so we can't make it accessible with reflection)
    )
  }

  /**
   * Extend the provided [LogicalPlan] with an annotation column.
   * 
   * see annotate(DataFrame) 

   * @param   plan  The [LogicalPlan] to anotate
   * @return        [plan] extended to produce an annotation column
   **/
  def annotate(plan:LogicalPlan): LogicalPlan = 
  {
    return AnnotatePlan(plan)
  }


  def allAttributeAnnotationsExpression: Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(ANNOTATION_COLUMN),
      Literal(COLUMN_ANNOTATION)
    )

  def attributeAnnotationExpression(attr: Attribute): Expression =
    UnresolvedExtractValue(
      allAttributeAnnotationsExpression,
      Literal(attr.name)
    )

  def rowAnnotationExpression: Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(ANNOTATION_COLUMN),
      Literal(ROW_ANNOTATION)
    )

  def annotationStruct(baseSchema:StructType): StructType =
  {
    StructType(Seq(
      StructField(ROW_ANNOTATION, BooleanType, false),
      StructField(COLUMN_ANNOTATION, StructType(
        baseSchema.fieldNames.map { 
          StructField(_, BooleanType, false)
        }
      ), false)
    ))
  }
}
