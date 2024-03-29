package org.mimirdb.caveats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.specs2.mutable.Specification
import org.mimirdb.caveats.implicits._
import org.mimirdb.test._

class EnumerateSpec
  extends Specification
  with SharedSparkTestInstance
{

  lazy val testDF:DataFrame = 
    dfr.select( 
          col("A"), 
          col("b").caveatIf(concat(lit("Hello "), col("C")), col("C") > 2) as "B" 
        )
       .caveatIf("World", col("A") === 4)

  def caveats(row: Boolean = true, attributes: Set[String] = Set("A", "B")) =
    testDF.listCaveats(row = row, attributes = attributes).map { _.message }

  def caveatSets(row: Boolean = true, attributes: Set[String] = Set("A", "B")) =
    testDF.listCaveatSets(row = row, attributes = attributes)

  "Enumerate Caveat Sets" >> {
    caveatSets() must haveSize(2)
  }

  "Enumerate Caveats" >> {
    caveats() must contain(exactly("Hello 3", "Hello 4", "World"))
  }

  "Enumerate Specific Caveats" >> {
    caveats(row = false, attributes = Set("B")) must contain(exactly("Hello 3", "Hello 4"))
    caveats(row = false, attributes = Set("A", "B")) must contain(exactly("Hello 3", "Hello 4"))
    caveats(row = false, attributes = Set("A", "b")) must contain(exactly("Hello 3", "Hello 4"))
    caveats(row = false, attributes = Set("A")) must beEmpty
  }

  "Enumerate Row Caveats" >> {
    caveats(row = true, attributes = Set()) must contain(exactly("World"))
  }

  "Enumerate Joins" >> {
    testDF.select(
      testDF("A") as "C",
      testDF("B") as "D"
    ).join(testDF, col("B") === col("C"))
     .listCaveats(row = false, attributes = Set("B", "C"))
     .map { _.message } must contain(exactly("Hello 3", "Hello 4"))
  }

  "Enumerate Left Outer Joins" >> {
    testDF.select(
      testDF("A") as "C",
      testDF("B") as "D"
    ).join(testDF, col("B") === col("C"), "leftouter")
     .listCaveats(row = false, attributes = Set("B", "C"))
     .map { _.message } must contain(exactly("Hello 3", "Hello 4"))
  }

  "Enumerate Joins with Cross-join Expressions" >> {
    val df = 
      testDF.select(
        testDF("A") as "C",
        testDF("B") as "D"
      ).join(testDF, col("B") === col("C"), "inner")
       .select(col("B"), col("C"), col("A") + col("D") as "E")



    df.listCaveats(
        row = false,
        constraint = df("E") === 3
      ).map { _.message } must contain(exactly("Hello 3", "Hello 3", "Hello 4"))
  }


}
