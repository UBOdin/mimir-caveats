package org.mimirdb.spark

import org.specs2.mutable.Specification
import org.apache.spark.sql.types._
import play.api.libs.json._
import scala.collection.mutable.ArraySeq


class SparkPrimitiveSpec
  extends Specification
{

  "De/Encode Arrays" >> {
    val data = ArraySeq(1, 2, 3, 4).map { new Integer(_) }
    val t = ArrayType(IntegerType, false)
    val encoded = SparkPrimitive.encode(data, t)
    encoded must haveClass[JsArray]
    encoded.as[Seq[Int]] must beEqualTo(Seq(1, 2, 3, 4))
    val decoded = SparkPrimitive.decode(encoded, t)
    decoded must beEqualTo(data)
  }

  "Decode Nulls" >> {
    SparkPrimitive.decode(JsNull, StringType) must beNull
  }
}
