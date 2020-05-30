package org.mimirdb.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import org.apache.spark.sql.catalyst.expressions._
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchersImplicits._
import org.mimirdb.utility.Bag
import org.mimirdb.spark.sparkWorkarounds._

trait DataFrameMatchers {

  def dfBagEquals(l: DataFrame, r: DataFrame): Boolean = {
    val lbag = Bag(l)
    val rbag = Bag(r)
    val schemaSame = l.schema.fields.map( _.name ).toSeq
      .equals(r.schema.fields.map( _.name ).toSeq)
    val bagSame = lbag.equals(rbag)
    schemaSame && bagSame
  }

  def schemaDifferences(l: DataFrame, r: DataFrame, considerDTs: Boolean = false): String = {
"schema differences" + l.schema.diff(r.schema).union(r.schema.diff(l.schema)).mkString("")
      if(considerDTs) {
        l.schema.diff(r.schema).union(r.schema.diff(l.schema)).mkString("\n")
      }
      else {
        val lNames = l.schema.fields.map( _.name ).toSet
        val rNames = r.schema.fields.map( _.name ).toSet
        val schemaSame = lNames.equals(rNames)
        if(!schemaSame) {
          lNames.diff(rNames).union(rNames.diff(lNames)).mkString("\n")
        } else ""
      }
  }

  def beBagEqualsTo(cmp: DataFrame): Matcher[DataFrame] = {
    d: DataFrame =>
    (
      dfBagEquals(d,cmp),
      s"""ACTUAL RESULT:\n${d.toShowString()}\nNOT THE SAME AS EXPECTED RESULT\n\n${cmp.toShowString()}\n\nSCHEMA DIFFERENCES:\n${schemaDifferences(d,cmp)}\n\nDATA DIFFERENCES:\n${Bag.listDifferences(Bag(d), Bag(cmp), "Actual","Expected")}"""
    )
  }

  def beBagEqualsTo(cmp: String): Matcher[DataFrame] = {
    val o = DataFramesSerializationParser.parseDF(cmp).castValuesAsStrings()
    d: DataFrame =>
    (
      dfBagEquals(d,o),
      s"""ACTUAL RESULT:\n${d.toShowString()}\nNOT THE SAME AS EXPECTED RESULT\n\n${o.toShowString()}\n\nSCHEMA DIFFERENCES:\n${schemaDifferences(d,o)}\n\nDATA DIFFERENCES:\n${Bag.listDifferences(Bag(d), Bag(o), "Actual","Expected")}"""
    )
  }

  def dfOrderedEqualsTo(l: DataFrame, r: DataFrame): Boolean = {
    val schemaSame = l.schema.fields.map( _.name ).toSeq
      .equals(r.schema.fields.map( _.name ).toSeq)
    val rowSame = l.collect().sameElements(r.collect())
    if (!rowSame) println(showDifference(l,r))
    schemaSame && rowSame
  }

  private def showDifference(l: DataFrame, r: DataFrame, checkDTs: Boolean = false): String = {
    val lrows = l.collect()
    val rrows = r.collect()
    val longer = if (lrows.length > rrows.length) lrows else rrows
    val other = if (lrows.length > rrows.length) rrows else lrows
    val schemaDiff =
      if(checkDTs) {
        "schema differences" + l.schema.diff(r.schema).union(r.schema.diff(l.schema)).mkString("") + "\n"
      }
      else {
        val lNames = l.schema.fields.map( _.name ).toSet
        val rNames = r.schema.fields.map( _.name ).toSet
        val schemaSame = lNames.equals(rNames)
        if(!schemaSame) {
          lNames.diff(rNames).union(rNames.diff(lNames))
        } else ""
      }

    val joinedrows = lrows.zip(rrows)
    val diffrows = joinedrows.filter { case (x,y) => !x.equals(y) }.map{ case (x,y) => s"row $x is not equal to $y" }.mkString("\n")

    schemaDiff +
    diffrows +
    longer.slice(other.size, longer.size).map( x => s"row $x only in larger DF" ).mkString("\n")
  }

  def beOrderedEqualsTo(cmp: DataFrame): Matcher[DataFrame] = {
    d: DataFrame =>
    (
      dfOrderedEqualsTo(d,cmp),
      s"${d.toShowString()}\n\n NOT THE SAME AS\n\n${cmp.toShowString()}\n"
    )
  }

  def beOrderedEqualsTo(cmp: String): Matcher[DataFrame] = {
    val o = DataFramesSerializationParser.parseDF(cmp, preserveNulls = true)
    d: DataFrame =>
    {
      d.plan
      val d2 = d.castValuesAsStrings()
      (
        dfOrderedEqualsTo(d2,o),
        s"${d2.toShowString()}\n\n NOT THE SAME AS\n\n${o.toShowString()}\n"
      )
    }
  }



}
