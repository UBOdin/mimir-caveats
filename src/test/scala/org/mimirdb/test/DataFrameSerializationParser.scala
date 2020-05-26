package org.mimirdb.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column, Row }

object DataFramesSerializationParser {

  def splitFields(line: String): Seq[String] = {
    line.trim.stripPrefix("|").stripSuffix("|")
      .split("[|]").map( x => x.trim())
  }

  def parseDF(ser: String): (Seq[String], Seq[Seq[String]]) = {
    val lines = ser.trim().split("\n")
    val dataLines = lines
      .filterNot( x => x.trim.matches("[+-]+"))
      .map(splitFields)
    val schema = dataLines(0)
    val data = dataLines.slice(1,dataLines.length)
    (schema, data)
  }

}
