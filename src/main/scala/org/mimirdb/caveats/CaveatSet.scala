package org.mimirdb.caveats

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.mimirdb.spark.sparkWorkarounds._
import org.mimirdb.spark.expressionLogic.negate


abstract class CaveatSet
{
  def size(ctx: SparkSession): Long
  def take(ctx: SparkSession, n: Int): Seq[Caveat]
  def all(ctx: SparkSession): Seq[Caveat]
  def isEmpty(ctx: SparkSession): Boolean
  def isEmptyExpression: Expression
  def isNonemptyExpression = negate(isEmptyExpression)
}


class SingletonCaveatSet(caveat: Caveat)
  extends CaveatSet
{
  def size(ctx: SparkSession) = 1
  def take(ctx: SparkSession, n:Int) = if(n > 0){ Seq(caveat) } else { Seq() }
  def all(ctx: SparkSession) = Seq(caveat)
  def isEmpty(ctx: SparkSession) = false
  def isEmptyExpression = Literal(false)
}

class EnumerableCaveatSet(
  plan: LogicalPlan, 
  family: Option[String]
) extends CaveatSet
{

  def withContext[T](ctx: SparkSession)(op: DataFrame => T): T =
    op(new DataFrame(ctx, plan, RowEncoder(plan.schema)))

  def size(ctx: SparkSession) = 
    withContext(ctx) { _.count() }
  def take(ctx: SparkSession, n:Int) = 
    withContext(ctx) { _.take(n).map { Caveat(family, _) } }
  def all(ctx: SparkSession) = 
    withContext(ctx) { _.collect.map { Caveat(family, _) } }
  def isEmpty(ctx: SparkSession) = 
    withContext(ctx) { _.isEmpty }
  def isEmptyExpression = Exists(plan)
}