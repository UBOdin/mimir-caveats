package mimir.caveats

import org.apache.spark.sql.{ Dataset, Row, Encoder }
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object Caveats
{

  def annotate(dataset:Dataset[Row]): Dataset[Row] = 
  {
    val execState = dataset.queryExecution
    val plan = execState.logical
    val annotated = annotate(plan)
    return new Dataset[Row](
      execState.sparkSession,
      annotated,
      RowEncoder(execState.analyzed.schema) 
      // ^---- UUUUGLY.  We should really be using dataset.encoder, but it's PRIVATE!!!!
      //       (and final, so we can't make it accessible with reflection)
    )
  }

  def annotate(plan:LogicalPlan): LogicalPlan = 
  {
    return AnnotatePlan(plan)
  }
}
