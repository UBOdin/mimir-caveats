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
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, BooleanType }

/**
  * Instrumenting queries to support range caveats which record for
  * each value an lower and upper bound across all possible worlds.
  * Furthermore, for each row we record and upper / lower bound on its
  * annotation (multiplicity) across all worlds.
  **/
object RangeCaveats
{
  val ANNOTATION_COLUMN = "__CAVEATS"
  val ROW_ANNOTATION = "ROW"
  val COLUMN_ANNOTATION = "COLUMN"
  val LOWER_BOUND = "LB"
  val BEST_GUESS = "BG"
  val UPPER_BOUND = "UB"

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
    )
  }

  def annotate(plan:LogicalPlan): LogicalPlan =
  {
    return RangeAnnotatePlan(plan)
  }

  def allAttributeAnnotationsExpression: Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(ANNOTATION_COLUMN),
      Literal(COLUMN_ANNOTATION),
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

  def lbExpression(e: Expression): Expression =
    UnresolvedExtractValue(
      e,
      Literal(LOWER_BOUND)
    )

  def bgAttrExpression(a: Attribute): Expression =
    a //TODO do we want this to work like this?

  def bgRowExpression(e: Expression): Expression =
    UnresolvedExtractValue(
      e,
      Literal(BEST_GUESS)
    )

  def ubExpression(e: Expression): Expression =
    UnresolvedExtractValue(
      e,
      Literal(UPPER_BOUND)
    )

  /**
    *  Schema of the caveat column is:
    *  ROW_ANNOTATION
    *      LOWER_BOUND
    *      BEST_GUESS
    *      UPPER_BOUND
    *  COLUMN_ANNOTATION
    *      A1
    *           LOWER_BOUND
    *           UPPER_BOUND
    *      ...
    *      An
    *           LOWER_BOUND
    *           UPPER_BOUND
    */
  def annotationStruct(baseSchema:StructType): StructType =
  {
    StructType(Seq(
      StructField(ROW_ANNOTATION,
        StructType(Seq(
          StructField(LOWER_BOUND,IntegerType, false),
          StructField(BEST_GUESS,IntegerType, false),
          StructField(UPPER_BOUND,IntegerType, false)
        )), false),
      StructField(COLUMN_ANNOTATION, StructType(
        baseSchema.fields.map { x =>
          StructField(x.name,
            StructType(Seq(
              StructField(LOWER_BOUND, x.dataType, false),
//              StructField(BEST_GUESS, _.dataType, false),
              StructField(UPPER_BOUND, x.dataType, false)
            ))
          )
        }
      ), false)
    ))
  }

}
