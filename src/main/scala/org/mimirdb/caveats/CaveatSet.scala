package org.mimirdb.caveats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions._
import org.mimirdb.spark.sparkWorkarounds._

abstract class CaveatSet
{
  def size: Long
  def take(n: Int): Seq[Caveat]
  def isEmpty: Boolean
  def isEmptyExpression: Expression
}


class SingletonCaveatSet(caveat: Caveat)
  extends CaveatSet
{
  def size = 1
  def take(n:Int) = if(n > 0){ Seq(caveat) } else { Seq() }
  def isEmpty = false
  def isEmptyExpression = Literal(false)
}

class EnumberableCaveatSet(
  source: DataFrame, 
  family: Option[String]
) extends CaveatSet
{
  def size = source.count()
  def take(n:Int) = source.take(n).map { Caveat(family, _) }
  def isEmpty = source.isEmpty
  def isEmptyExpression = Exists(source.plan)
}