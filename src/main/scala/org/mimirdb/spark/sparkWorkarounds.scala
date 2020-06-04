package org.mimirdb.spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BinaryType

class DataFrameImplicits(df:DataFrame)
{
  /**
   * DataFrame's .withPlan is private :[
   *
   * Use the power of implicits to create our own version
   */
  def extend( plan: LogicalPlan ) =
  {
    new DataFrame(df.sparkSession, plan, RowEncoder(plan.schema))
  }

  def plan = df.queryExecution.analyzed

  /**
    *  Creates a dataframe from a logical plan and calls the analyzer if necessary to ensure that we have a valid row encoder. This safes us from always having to create the result schema (including correct datatypes manually.
    */
  def planToDF(logicalPlan: LogicalPlan): DataFrame = {
    df.sparkSession
    val qe = df.sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new DataFrame(df.sparkSession, logicalPlan, RowEncoder(qe.analyzed.schema))
  }

  /**
    * copied useful private methods and made them available under different name for our convenience.
    */
  def toShowString(
    _numRows: Int = 10,
    truncate: Int = 20,
    vertical: Boolean = false): String =
  {
    val numRows = _numRows.max(0).min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH - 1)
    // Get rows represented by Seq[Seq[String]], we may get one more line if it has more data.
    val tmpRows = getStringRows(numRows, truncate)

    val hasMoreData = tmpRows.length - 1 > numRows
    val rows = tmpRows.take(numRows + 1)

    val sb = new StringBuilder
    val numCols = df.schema.fieldNames.length
    // We set a minimum column width at '3'
    val minimumColWidth = 3

    if (!vertical) {
      // Initialise the width of each column to a minimum value
      val colWidths = Array.fill(numCols)(minimumColWidth)

      // Compute the width of each column
      for (row <- rows) {
        for ((cell, i) <- row.zipWithIndex) {
          colWidths(i) = math.max(colWidths(i), SparkHelpers.stringHalfWidth(cell))
        }
      }

      val paddedRows = rows.map { row =>
        row.zipWithIndex.map { case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(cell, colWidths(i) - SparkHelpers.stringHalfWidth(cell) + cell.length)
          } else {
            StringUtils.rightPad(cell, colWidths(i) - SparkHelpers.stringHalfWidth(cell) + cell.length)
          }
        }
      }

      // Create SeparateLine
      val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

      // column names
      paddedRows.head.addString(sb, "|", "|", "|\n")
      sb.append(sep)

      // data
      paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
      sb.append(sep)
    } else {
      // Extended display mode enabled
      val fieldNames = rows.head
      val dataRows = rows.tail

      // Compute the width of field name and data columns
      val fieldNameColWidth = fieldNames.foldLeft(minimumColWidth) { case (curMax, fieldName) =>
        math.max(curMax, SparkHelpers.stringHalfWidth(fieldName))
      }
      val dataColWidth = dataRows.foldLeft(minimumColWidth) { case (curMax, row) =>
        math.max(curMax, row.map(cell => SparkHelpers.stringHalfWidth(cell)).max)
      }

      dataRows.zipWithIndex.foreach { case (row, i) =>
        // "+ 5" in size means a character length except for padded names and data
        val rowHeader = StringUtils.rightPad(
          s"-RECORD $i", fieldNameColWidth + dataColWidth + 5, "-")
        sb.append(rowHeader).append("\n")
        row.zipWithIndex.map { case (cell, j) =>
          val fieldName = StringUtils.rightPad(fieldNames(j),
            fieldNameColWidth - SparkHelpers.stringHalfWidth(fieldNames(j)) + fieldNames(j).length)
          val data = StringUtils.rightPad(cell,
            dataColWidth - SparkHelpers.stringHalfWidth(cell) + cell.length)
          s" $fieldName | $data "
        }.addString(sb, "", "\n", "\n")
      }
    }

    // Print a footer
    if (vertical && rows.tail.isEmpty) {
      // In a vertical mode, print an empty row set explicitly
      sb.append("(0 rows)\n")
    } else if (hasMoreData) {
      // For Data that has more than "numRows" records
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

  def castValuesAsStrings(): DataFrame  = {
    SparkHelpers.castDFValuesAsStrings(df)
    // val castCols = df.queryExecution.logical.output.map { col =>
    //   // Since binary types in top-level schema fields have a specific format to print,
    //   // so we do not cast them to strings here.
    //   if (col.dataType == BinaryType) {
    //     new Column(col)
    //   } else {
    //     new Column(col).cast(StringType)
    //   }
    // }
    // df.select(castCols: _*)
  }

  def getStringRows(
      numRows: Int,
      truncate: Int): Seq[Seq[String]] = {
    val newDf = df.toDF()
    val data = SparkHelpers.castDFValuesAsStrings(df).take(numRows + 1)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    df.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }
  }
}

object sparkWorkarounds
{
  implicit def DataFrameToMimirDataFrame(df:DataFrame) =
    new DataFrameImplicits(df)
}

object SparkHelpers
{

  def planToDF(plan: LogicalPlan, sparkSession: SparkSession): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(plan)
    qe.assertAnalyzed()
    new DataFrame(sparkSession, plan, RowEncoder(qe.analyzed.schema))
  }

  def castDFValuesAsStrings(df: DataFrame): DataFrame  = {
    val castCols = df.queryExecution.analyzed.output.map { col =>
      // Since binary types in top-level schema fields have a specific format to print,
      // so we do not cast them to strings here.
      if (col.dataType == BinaryType) {
        new Column(col)
      } else {
        new Column(col).cast(StringType)
      }
    }
    df.select(castCols: _*)
  }

  def stringHalfWidth(str: String): Int = {
    if (str == null) 0 else str.length + fullWidthRegex.findAllIn(str).size
  }

  private val fullWidthRegex = ("""[""" +
    // scalastyle:off nonascii
    """\u1100-\u115F""" +
    """\u2E80-\uA4CF""" +
    """\uAC00-\uD7A3""" +
    """\uF900-\uFAFF""" +
    """\uFE10-\uFE19""" +
    """\uFE30-\uFE6F""" +
    """\uFF00-\uFF60""" +
    """\uFFE0-\uFFE6""" +
    // scalastyle:on nonascii
    """]""").r

}
