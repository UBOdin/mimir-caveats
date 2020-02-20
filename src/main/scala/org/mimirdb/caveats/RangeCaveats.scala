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

import org.mimirdb.caveats.annotate._
import org.mimirdb.caveats.Constants._

/**
  * Instrumenting queries to support range caveats which record for
  * each value an lower and upper bound across all possible worlds.
  * Furthermore, for each row we record and upper / lower bound on its
  * annotation (multiplicity) across all worlds.
  **/
object RangeCaveats
{

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
          ANNOTATION_ATTRIBUTE,
          annotationStruct(plan.schema),
          false
        )
      )
    )
  }

  def annotate(plan:LogicalPlan): LogicalPlan =
  {
    return CaveatRangePlan(plan)
  }

  def allAttributeAnnotationsExpression: Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(ANNOTATION_ATTRIBUTE),
      Literal(ATTRIBUTE_FIELD),
    )

  def attributeAnnotationExpression(attr: Attribute): Expression =
    UnresolvedExtractValue(
      allAttributeAnnotationsExpression,
      Literal(attr.name)
    )

  def rowAnnotationExpression: Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(ANNOTATION_ATTRIBUTE),
      Literal(ROW_FIELD)
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
  def annotationStruct(baseSchema:StructType): StructType =
  {
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

}
