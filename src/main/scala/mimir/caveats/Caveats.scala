package mimir.caveats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Literal,
  Attribute
}
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedExtractValue,
  UnresolvedAttribute
}

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
    return new DataFrame(
      execState.sparkSession,
      annotated,
      ???
      ///RowEncoder(annotated.queryExecution.analyzed.schema) 
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
      attr
    )

  def rowAnnotationExpression: Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(ANNOTATION_COLUMN),
      Literal(ROW_ANNOTATION)
    )

}
