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

  def beBagEqualsTo(cmp: DataFrame): Matcher[DataFrame] = {
    d: DataFrame =>
    (
      dfBagEquals(d,cmp),
      s"""${d.toShowString()}\n\n NOT THE SAME AS\n\n${cmp.toShowString()}\n${Bag.listDifferences(Bag(d), Bag(cmp), "Actual","Expected")}"""
    )
  }

  def beBagEqualsTo(cmp: String): Matcher[DataFrame] = {
    val o = DataFramesSerializationParser.parseDF(cmp).castValuesAsStrings()
    d: DataFrame =>
    (
      dfBagEquals(d,o),
      s"""${d.toShowString()}\n\n NOT THE SAME AS\n\n${o.toShowString()}\n${Bag.listDifferences(Bag(d), Bag(o), "Actual","Expected")}"""
    )
  }

  def dfOrderedEqualsTo(l: DataFrame, r: DataFrame): Boolean = {
    l.schema.equals(r.schema) && l.collect().sameElements(r.collect())
  }

  def beOrderedEqualsTo(cmp: DataFrame): Matcher[DataFrame] = {
    d: DataFrame =>
    (
      dfOrderedEqualsTo(d,cmp),
      s"$d != $cmp\n"
    )
  }

  def beOrderedEqualsTo(cmp: String): Matcher[DataFrame] = {
    d: DataFrame =>
    (
      dfOrderedEqualsTo(d,DataFramesSerializationParser.parseDF(cmp)),
      s"$d != $cmp\n"
    )
  }



}
