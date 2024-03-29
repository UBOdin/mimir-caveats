
package org.mimirdb.caveats

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical._
import org.mimirdb.spark.sparkWorkarounds._
import org.mimirdb.spark.expressionLogic.negate
import com.typesafe.scalalogging.LazyLogging


abstract class CaveatSet
{
  def size(ctx: SparkSession): Long
  def take(ctx: SparkSession, n: Int): Seq[Caveat]
  def all(ctx: SparkSession): Seq[Caveat]
  def isEmpty(ctx: SparkSession): Boolean
  def isEmptyExpression: Expression
  def isNonemptyExpression = negate(isEmptyExpression)
}


class SingletonCaveatSet(val caveat: Caveat)
  extends CaveatSet
{
  def size(ctx: SparkSession) = 1
  def take(ctx: SparkSession, n:Int) = if(n > 0){ Seq(caveat) } else { Seq() }
  def all(ctx: SparkSession) = Seq(caveat)
  def isEmpty(ctx: SparkSession) = false
  def isEmptyExpression = Literal(false)


  override def toString() = 
    s"SINGLETON CAVEAT $caveat"
}

class EnumerableCaveatSet(
  val plan: LogicalPlan, 
  val family: Option[String]
) extends CaveatSet
  with LazyLogging
{

  def withContext[T](ctx: SparkSession)(op: DataFrame => T): T =
    op(new DataFrame(ctx, Caveats.strip(plan), RowEncoder(plan.schema)))

  def size(ctx: SparkSession) = 
    withContext(ctx) { _.count() }
  def take(ctx: SparkSession, n:Int) = 
    withContext(ctx) { _.take(n).map { Caveat(family, _) } }
  def all(ctx: SparkSession) = 
  {
    logger.trace(s"BEFORE OPTIMIZATION: \n$plan")
    withContext(ctx) { df =>
      logger.trace(s"AFTER OPTIMIZATION: \n${df.queryExecution.optimizedPlan}")
      df.collect.map { Caveat(family, _) } 
    }
  }
  def isEmpty(ctx: SparkSession) = 
    withContext(ctx) { _.isEmpty }

  // Exists would be fantastic to use here, but unfortunately Spark doesn't like Exists in 
  // projections.  To work around, compare to 0
  def isEmptyExpression = 
    EqualTo(
      ScalarSubquery(
        Aggregate(Seq(), Seq(
          Alias(Count(Seq(Literal(1))).toAggregateExpression(false), "num_caveats")()
        ), plan)
      ),
      Literal(0)
    )

  override def toString() = 
    "ENUMERABLE CAVEAT" + (family.map { " ("+_+")" }.getOrElse("")) + "\n" + 
      plan.toString + "\n----\n" +
      Caveats.strip(plan)
}