package org.mimirdb.caveats

import org.apache.spark.sql.functions._
import org.specs2.mutable.Specification
import org.mimirdb.caveats.implicits._
import org.mimirdb.test._

class GroupByAggSpec
  extends Specification
  with SharedSparkTestInstance
{

  lazy val testDF = 
    dfr.select( 
          col("A").cast("int")
                  .caveatIf("Hello", col("c") === 1) as "A",
          col("B").cast("int") as "B"
        )
       .caveatIf("World", col("A") === 4)

  "Filter caveats for one layer of aggregation" >> {

    dfr.show()

    val grouped =
      testDF.groupBy("b").sum("A")

    grouped.show()

    val caveatFlags = 
      grouped.trackCaveats
             .collect()
             .filter { row => !row.isNullAt(0) }
             .map { row => 
                row.getInt(0) -> 
                (
                  // Row Caveat
                  row.getStruct(2)
                     .getBoolean(0),
                  // Sum(A) caveat
                  row.getStruct(2)
                     .getStruct(1)
                     .getBoolean(1)
                )
             }.toMap

    caveatFlags must contain( 2 -> (false, true) )

    val caveatsOnTwo:Seq[String] =
      grouped.filter(col("B") === 2)
             .listCaveats()
             .map { caveat => 
               caveat.message
             }

    caveatsOnTwo must contain(eachOf("Hello", "World"))
    caveatsOnTwo.filter { _.equals("Hello") }.size must beEqualTo(1)

  }
}
