package org.mimirdb.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column, Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object DataFramesSerializationParser
    extends SharedSparkTestInstance
{

  import spark.implicits._

  def splitFields(line: String): Seq[String] = {
    line.trim.stripPrefix("|").stripSuffix("|")
      .split("[|]").map( x => x.trim())
  }

  def parseDF(ser: String): DataFrame = {
    val (schema, data) = parseDFasTable(ser)
    val sparkSchema = StructType(schema.map( name => StructField(name, StringType, true) ))
    val sparkData = data.map( s => Row.fromSeq(s) )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(sparkData),
      sparkSchema
    )
    df.queryExecution.analyzed
    df
  }

  def parseDFasTable(ser: String): (Seq[String], Seq[Seq[String]]) = {
    val lines = ser.trim().split("\n")
    val dataLines = lines
      .filterNot( x => x.trim.matches("[+-]+"))
      .map(splitFields)
    val schema = dataLines(0)
    val data = dataLines.slice(1,dataLines.length)
    (schema, data)
  }

}
