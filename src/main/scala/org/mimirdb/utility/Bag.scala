package org.mimirdb.utility

import org.apache.spark.sql. { DataFrame, Row }
import org.mimirdb.spark.sparkWorkarounds._
import scala.reflect.ClassTag

case class Bag[T](data: Map[T,Int])(implicit val tag: ClassTag[T]) {

  def union(other: Bag[T]): Bag[T] = {
    var newdata: Map[T,Int] = this.data
    other.data.foreach {
      case (v,mult) =>
        if (newdata.contains(v))
          newdata = newdata + (v -> (newdata(v) + mult))
      else newdata = newdata + (v -> mult)
    }
    Bag(newdata)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case x: Bag[_] => x.tag == tag && Bag.bagEquals(this,x.asInstanceOf[Bag[T]])
      case _ =>  false
    }
  }

}

object Bag {

  def apply[T](in: Seq[T])(implicit tag: ClassTag[T]): Bag[T] = {
    val data = in.groupBy{ x => x }.map { case (k, v) => k -> v.map( x => 1).reduce( (x,y) => x + y ) }
    Bag[T](data)
  }

  def rowSeqBagFromDF(d: DataFrame): Bag[Row] = {
    val rows = d.castValuesAsStrings().collect().toSeq
    Bag(rows)
  }

  def apply(d: DataFrame): Bag[Seq[String]] = {
    val rows = d.collect().map {
      r => rowToStringSeq(r)
      }.toSeq
    Bag(rows)
  }

  def rowToStringSeq(r: Row): Seq[String] = {
    r.toSeq.map { x =>
      x match {
        case null => "null"
        case _ => x.toString()
      }
    }
  }

  def bagEquals[T](l: Bag[T], r: Bag[T]): Boolean = {
    val (ld,rd) = (l.data, r.data)

    ld.size == rd.size && (ld.forall { case (k,v) => rd.contains(k) && rd(k) == v }) && (rd.forall { case (k,v) => ld.contains(k) && ld(k) == v })
  }

  def listDifferences[T](l: Bag[T], r: Bag[T], leftName: String = "left", rightName: String = "right"): String = {
    val (ld,rd) = (l.data, r.data)

    val res = ld.size == rd.size && (ld.forall { case (k,v) => rd.contains(k) && rd(k) == v }) && (rd.forall { case (k,v) => ld.contains(k) && ld(k) == v })

    if (!res) {
      val conflicting = ld.filter{ case(k,v) => rd.contains(k) && rd(k) != v }.map{ case (k,v) => (k,(v,rd(k))) }
      val onlyLeft = ld.filterNot { case (k,v) => rd.contains(k)  }
      val onlyRight = rd.filterNot { case (k,v) => ld.contains(k) }

      s"$leftName: $l\n" + s"$rightName: $r\n" + s"Elements with different multiplicity: $conflicting\n" + s"Only in $leftName: $onlyLeft\n" + s"Only in $rightName: $onlyRight\n"
    }
    else
      ""
  }

}
