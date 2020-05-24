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

/**
  * main entry point for caveat rewriting that dispatches to a particular [AnnotationInstrumentationStrategy]
  * for a particular [AnnotationType].
  */
object Caveats
{

  var defaultAnnotator: AnnotationInstrumentationStrategy = CaveatExists()

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
    annotator: AnnotationInstrumentationStrategy = defaultAnnotator,
    annotationAttribute: String = ANNOTATION_ATTRIBUTE
  ): DataFrame =
  {
    val execState = dataset.queryExecution
    val plan = execState.analyzed
    val annotated = annotator(plan)
    val baseSchema = plan.schema
    val annotSchema = if (baseSchema.fields.exists(_.name == annotationAttribute)) baseSchema else
          annotator.outputEncoding.annotationStruct(baseSchema, annotationAttribute)

    return new DataFrame(
      execState.sparkSession,
      annotated,
      RowEncoder(annotSchema)
    )
  }

  def planIsAnnotated(plan: LogicalPlan, annotation: String = ANNOTATION_ATTRIBUTE): Boolean =
    plan.output.map { _.name }.exists { _.equals(annotation) }

  def translateUncertainToAnnotation(
    df: DataFrame,
    model: UncertaintyModel,
    annotator: AnnotationInstrumentationStrategy = defaultAnnotator,
    annotationAttribute: String = ANNOTATION_ATTRIBUTE
  ): DataFrame = {
    val plan = df.queryExecution.analyzed
    val annotated = annotator.translateFromUncertaintyModel(plan, model)
    val normalAttrs = model.adaptedSchema(plan.schema)
    val rowEncoder = RowEncoder(
          annotator.outputEncoding.annotationStruct(normalAttrs)
        )

    println(normalAttrs)
    println(rowEncoder)
    return new DataFrame(
      df.queryExecution.sparkSession,
      annotated,
      rowEncoder
    )
  }

  def strip(plan: LogicalPlan): LogicalPlan =
  {
    plan.transformAllExpressions {
      case ApplyCaveat(value, _, _, _, _, _) => value
    }
  }

  def strip(df: DataFrame): DataFrame =
  {
    val stripped = strip(df.queryExecution.analyzed)
    return new DataFrame(
      df.queryExecution.sparkSession,
      stripped,
      RowEncoder(stripped.schema)
    )

  }

}
