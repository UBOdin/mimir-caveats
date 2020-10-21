package org.mimirdb.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import org.mimirdb.caveats.{ PrettyPrint, Caveat }

object SharedSparkTestInstance
{
  lazy val spark = {
    PrettyPrint.simpleOutput = true
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      .getOrCreate()
  }
  lazy val dfr = /* R(A int, B int, C int) */
    spark.read
         .format("csv")
         .option("header", "true")
      .load("test_data/r.csv")
  lazy val dfs = /* S(D int, E int) */
    spark.read
      .format("csv")
      .option("header", "true")
      .load("test_data/s.csv")
  lazy val dft = /* T(F int, G int) */
    spark.read
      .format("csv")
      .option("header", "true")
      .load("test_data/t.csv")
  lazy val dftip = /* TIP(A int, B int, P float) */
    spark.read
      .format("csv")
      .option("header", "true")
      .load("test_data/tip.csv")
  lazy val tables = Seq(dfr,dfs,dft,dftip)
  val tableNames = Seq("r","s","t","tip")
  var tablesLoaded = false
  var udfsLoaded = false

  def registerSQLtables() = synchronized {
    if(!tablesLoaded) {
      tableNames.zip(tables).map{ case (name,table) => table.createTempView(name) }
      tablesLoaded = true
    }
  }
  def registerUDFs() = synchronized {
    if(!udfsLoaded) {
      Caveat.registerUDF(spark)
    }    
  }

}

trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark
  lazy val dfr = SharedSparkTestInstance.dfr
  lazy val dfs = SharedSparkTestInstance.dfs
  lazy val dft = SharedSparkTestInstance.dft
  lazy val dftip = SharedSparkTestInstance.dftip
  lazy val tables = Seq(dfr,dfs,dft,dftip)

  def registerSQLtables() = {
    SharedSparkTestInstance.registerSQLtables()
  }
}
