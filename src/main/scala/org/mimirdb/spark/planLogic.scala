package org.mimirdb.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import scala.collection.mutable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalog.Table

object planLogic
{

  def allocateFreshAttributes(plan: LogicalPlan): (LogicalPlan, Map[ExprId, Attribute]) =
  {
    val mappingCache = mutable.ArrayBuffer[(Attribute, Attribute)]()

    val rewritten = plan.transformUpWithNewOutput {
      case Project(projectList, child) => 
        var mapping = 
          projectList.map { 
            case a@Alias(expr, name) => 
              a -> Alias(expr, name)(qualifier = a.qualifier)
            case a:NamedExpression => 
              a -> Alias(a, a.name)(qualifier = a.qualifier)
          }
        val mappingAttributes = mapping.map { x => (x._1.toAttribute -> x._2.toAttribute) }
        mappingCache ++= mappingAttributes
        (
          Project(
            mapping.map { _._2 },
            child
          ), 
          mappingAttributes
        )
    }

    return (rewritten, mappingCache.map { case (k, v) => k.exprId -> v }.toMap)
  }

}