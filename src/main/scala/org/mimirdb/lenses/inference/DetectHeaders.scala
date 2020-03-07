package org.mimirdb.lenses.inference

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object DetectHeaders
{
  def apply(
    df: DataFrame, 
    detectedHint: Seq[StructField] = null
  ): Option[Seq[String]] =
  {
    // Get the first row.  If there is no first row, it's obviously not a header
    val firstRow = df.take(1).headOption.getOrElse { return None }

    // Really simple heuristic: Find any non-string columns and see if
    // the first row is suspiciously string-like.
    val nonStringColumns = 
      Option(detectedHint)
        .getOrElse{ InferTypes(df, 0.3) }
        .filter { !_.dataType.equals(StringType) }
        .map { _.name }

    def makeHeader = 
      Some(
        firstRow
          .toSeq
          .map { _.toString
                  .replaceAll("[^a-zA-Z0-9_]", "") // replace difficult chars
                  .replace("^([0-9])", "_\\1")     // prefix leading digits
               }
          .zipWithIndex
          .map {                          // replace empty strings
            case ("", idx) => "_c"+(idx)
            case (x , _)   => x
          }
      )

    if(nonStringColumns.isEmpty){
      // well, bla... we have nothing to go on.  YOLO time
      return makeHeader
    }

    val isStringInHeader = 
      InferTypes.testRow(firstRow)
                .filter { _._2.equals(StringType) }
                .map { _._1 }
                .toSet

    val headerTypeViolations = 
      nonStringColumns.toSet & isStringInHeader

    // Aggressively assume that any violations in the first row indicate 
    // a header
    if(headerTypeViolations.isEmpty) { return None }
    else { 
      return makeHeader
    }
  }
}