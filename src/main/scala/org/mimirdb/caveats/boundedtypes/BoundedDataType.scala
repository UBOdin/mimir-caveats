package org.mimirdb.caveats.boundedtypes

import org.apache.spark.sql.types._

import org.mimirdb.caveats.annotate._

trait BoundedDomain[T] {
  def minValue: T
  def maxValue: T
  def order: Ordering[T]
}

/**
  * The elements of Bounded data types are totally ordered and there exist a minimum, median, and maximum value.
  */
object BoundedDataType {

  /**
    * Returns minimum and maximum domain values for a datatype.
    *
    * @param d the [[DataType]] whose domain extrema we want to return
    */
  def domainMinAndMax(d: DataType): (Any,Any) = {
    d match {
      case IntegerType => (Int.MinValue, Int.MaxValue)
      case FloatType => (Float.MinValue, Float.MaxValue)
      case StringType => ???
      case BooleanType => (false, true)
      case _ => throw new AnnotationException(s"Datatype ${d} is not known to be bounded, cannot retrieve min and max values")
    }
  }

  def domainMinMedianMax(d: DataType): (Any,Any,Any) = {
    d match {
      case IntegerType => (Int.MinValue, (Int.MinValue + (Int.MaxValue / 2)), Int.MaxValue)
      case FloatType => (Float.MinValue, (Float.MinValue + (Float.MaxValue / 2)), Float.MaxValue)
      case StringType => ???
      case BooleanType => (false, false, true)
      case _ => throw new AnnotationException(s"Datatype ${d} is not known to be bounded or does not have a median, cannot retrieve min, median, and max values")
    }
  }

  def domainHasMedian(d: DataType): Boolean = {
    d match {
      case IntegerType | FloatType | BooleanType => true
      case _ => false
    }
  }

  def domainMin(d: DataType) : Any = {
    domainMinAndMax(d)._1
  }

  def domainMax(d: DataType) : Any = {
    domainMinAndMax(d)._2
  }

  def isBoundedType(d:DataType) : Boolean = {
    d match {
      case IntegerType | FloatType | BooleanType => true
      case _ => false
    }
  }

  def getOrder[T <: DataType](a: T): BoundedDomain[T] = {
    ???
  }

}
