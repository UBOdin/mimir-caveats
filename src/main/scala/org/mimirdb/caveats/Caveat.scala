package org.mimirdb.caveats

import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.AnalysisException
import org.mimirdb.caveats.annotate.AnnotationException
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.mimirdb.caveats.annotate.CaveatExistsInExpression

case class Caveat(
  message: String,
  family: Option[String],
  key: Seq[Literal]
)

object Caveat
{
  def apply(family: Option[String], config:Row): Caveat =
  {
    Caveat(
      message = config.getAs[String](Constants.MESSAGE_ATTRIBUTE),
      family  = family,
      key     = config.getAs[Seq[Any]](Constants.KEY_ATTRIBUTE).map { Literal(_) }
    )
  }

  /**
    * Names and UDFs for Caveating in SQL.
    *
    * @return the UDF name
    */
  def udfs = Seq(
    ("Caveat", caveatUDF _),
    ("CaveatIf", caveatIfUDF _),
    ("CaveatIfNull", caveatIfNullUDF _),
    ("CaveatReplaceIf", caveatReplaceIfUDF _),
    ("HasCaveat", hasCaveatUDF _)
  )

  /**
    * Default message to use for caveats if the user has not provided any.
    *
    * @return
    */
  def defaultMessage = Literal("Caveated Value")

  /**
    * [[FunctionBuilder]] that is registered as a UDF to replace Caveat UDF calls
    * with ApplyCaveat expressions.
    *
    *  Spark's UDF framework allow UDFs to be registered as FunctionBuilder:
    * Seq[Expression] => Expression which map Seq[Expression] (the children of
    * the UDFFunction expression node) to Expression. This is builder is applied
    * during analysis. We use it the replace the call the the Caveat UDF from
    * SQL with our custom ApplyCaveat type.
    *
    * @param children
    * @return the input (first child) wrapped into [ApplyCaveat]
    */
  def caveatUDF(children: Seq[Expression]): Expression =
    children match {
      // user has not provided a message
      case value +: Nil => ApplyCaveat(value, Literal(defaultMessage))
      // value and message
      case value +: message +: Nil => ApplyCaveat(value,message)
      // value, message, and key
      case value +: message +: key => ApplyCaveat(value,message,None,key)
        // should never get here
      case _ => throw new AnnotationException(
        """Caveat needs to be provided at least with an expression whose result the
          |caveat should be applied to like so Caveat(value).
          |
          |Other options are Caveat(value,message) where a  message is recorded for the caveated value and Caveat(value,message,keys...) which creates and identy for the caveat based on the values of the keys expressions.""".stripMargin)
    }

  /**
    * [[FunctionBuilder]] that is registered as a UDF to replace Caveat UDF calls
    * with ApplyCaveat expressions.
    *
    * @param children
    * @return the input (first child) wrapped into [ApplyCaveat] with condition
    * (second child)
    */
  def caveatIfUDF(children: Seq[Expression]): Expression =
    children match {
      // user has not provided a message
      case value +: cond +: Nil => ApplyCaveat(value, Literal(defaultMessage), condition = cond)
      // value and message
      case value +: cond +: message +: Nil => ApplyCaveat(value,message, condition = cond)
      // value, message, and key
      case value +: cond +: message +: key => ApplyCaveat(value,message,None,key, condition = cond)
        // should never get here
      case _ => throw new AnnotationException(
        """CaveatIf needs to be provided at least with an expression whose result the
          |caveat should be applied to and a condition like so Caveat(value, condition).
          |
          |Other options are Caveat(value,cond,message) where a  message is recorded for the caveated value and Caveat(value,cond,message,keys...) which creates and identy for the caveat based on the values of the keys expressions.""".stripMargin)
    }

  /**
    * [[FunctionBuilder]] that is registered as a UDF to replace Caveat UDF calls
    * with ApplyCaveat expressions.
    *
    * @param children
    * @return the input (first child) wrapped into [ApplyCaveat] with condition
    * that checks whether input is null.
    */
  def caveatIfNullUDF(children: Seq[Expression]): Expression =
    children match {
      // user has not provided a message
      case value +: Nil => ApplyCaveat(value, Literal(defaultMessage), condition = IsNull(value))
      // value and message
      case value +: message +: Nil => ApplyCaveat(value,message, condition = IsNull(value))
      // value, message, and key
      case value +: message +: key => ApplyCaveat(value,message,None,key, condition = IsNull(value))
        // should never get here
      case _ => throw new AnnotationException(
        """Caveat needs to be provided at least with an expression whose result the
          |caveat should be applied to like so Caveat(value).
          |
          |Other options are Caveat(value,message) where a  message is recorded for the caveated value and Caveat(value,message,keys...) which creates and identy for the caveat based on the values of the keys expressions.""".stripMargin)
    }

  def caveatReplaceIfUDF(children: Seq[Expression]): Expression =
    children match {
      // user has not provided a message
      case value +: replacement +: cond +: Nil => ApplyCaveat.replace(value, replacement, cond, Literal(defaultMessage))
      //  message
      case value +: replacement +:cond +: message +: Nil => ApplyCaveat.replace(value, replacement, cond, Literal(defaultMessage))
      // message, and key
      case value +: replacement +:cond +: message +: key => ApplyCaveat.replace(value, replacement, cond, Literal(defaultMessage), key)
        // should never get here
      case _ => throw new AnnotationException(
        """CaveatIf needs to be provided at least with an expression whose result the
          |caveat should be applied to and a condition like so Caveat(value, condition).
          |
          |Other options are Caveat(value,cond,message) where a  message is recorded for the caveated value and Caveat(value,cond,message,keys...) which creates and identy for the caveat based on the values of the keys expressions.""".stripMargin)
    }

  def hasCaveatUDF(children: Seq[Expression]): Expression =
    children match {
      case value +: Nil => HasCaveat(value)
      case _ => throw new AnnotationException(
        """CaveatIf needs to be provided at least with an expression whose result the
          |caveat should be applied to and a condition like so Caveat(value, condition).
          |
          |Other options are Caveat(value,cond,message) where a  message is recorded for the caveated value and Caveat(value,cond,message,keys...) which creates and identy for the caveat based on the values of the keys expressions.""".stripMargin)
    }

  /**
    * We register UDF that will be registered with Spark SQL to enable caveating
    * from SQL.
    *
    * @param s the SparkSession for which we want to register the UDF.
    */
  def registerUDF(s: SparkSession) = {
    udfs.map{ case (name,udf) =>  s.sessionState.functionRegistry.createOrReplaceTempFunction(name, udf) }
  }
}
