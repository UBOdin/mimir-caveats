package org.mimirdb.spark

import play.api.libs.json._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import java.util.{ Base64, Calendar }
import java.sql.{ Date, Timestamp }
import scala.util.matching.Regex

object SparkPrimitive
{
  def base64Encode(b: Array[Byte]): String =
    Base64.getEncoder().encodeToString(b)

  def base64Decode(b: String): Array[Byte] =
    Base64.getDecoder().decode(b)

  def formatDate(date: Date): String = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    val y = cal.get(Calendar.YEAR)
    val m = cal.get(Calendar.MONTH)
    val d = cal.get(Calendar.DAY_OF_MONTH)
    f"$y%04d-$m%02d-$d%02d"
  }

  def formatTimestamp(timestamp: Timestamp): String = {
    val cal = Calendar.getInstance()
    cal.setTime(timestamp)
    val y   = cal.get(Calendar.YEAR)
    val m   = cal.get(Calendar.MONTH)
    val d   = cal.get(Calendar.DAY_OF_MONTH)
    val hr  = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val sec = cal.get(Calendar.SECOND)
    val ms  = cal.get(Calendar.MILLISECOND)
    f"$y%04d-$m%02d-$d%02d $hr%02d:$min%02d:$sec%02d.$ms%03d"
  }

  val DateString = "([0-9]{4})-([0-9]{2})-([0-9]{2})".r
  val TimestampString = "([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9.]+)".r

  def decodeDate(date: String): Date = 
    date match {
      case DateString(y, m, d) => {
        val cal = Calendar.getInstance()
        cal.set(
          y.toInt, 
          m.toInt, 
          d.toInt, 
          0, 0, 0
        )
        new Date(cal.getTimeInMillis)
      }
      case _ => throw new IllegalArgumentException(s"Invalid Date: '$date'")
    }

  def decodeTimestamp(timestamp: String): Timestamp = 
    timestamp match {
      case TimestampString(y, m, d, hr, min, sec) => {
        val cal = Calendar.getInstance()
        val secWithMsec = sec.toFloat
        cal.set(
          y.toInt, 
          m.toInt, 
          d.toInt, 
          hr.toInt,
          min.toInt,
          secWithMsec.toInt
        )
        cal.set(((secWithMsec - secWithMsec.toInt) * 1000).toInt, Calendar.MILLISECOND)
        new Timestamp(cal.getTimeInMillis)
      }
      case _ => throw new IllegalArgumentException(s"Invalid Timestamp: '$timestamp'")
    }

  def encode(k: Any, t: DataType): JsValue =
  {
    t match {
      case _ if k == null       => JsNull
      case StringType           => JsString(k.toString)
      case BinaryType           => JsString(base64Encode(k.asInstanceOf[Array[Byte]]))
      case BooleanType          => JsBoolean(k.asInstanceOf[Boolean])
      case DateType             => JsString(formatDate(k.asInstanceOf[Date]))
      case TimestampType        => JsString(formatTimestamp(k.asInstanceOf[Timestamp]))
      case CalendarIntervalType => Json.obj(
                                      "months"       -> k.asInstanceOf[CalendarInterval].months,
                                      "days"         -> k.asInstanceOf[CalendarInterval].days,
                                      "microseconds" -> k.asInstanceOf[CalendarInterval].microseconds
                                    )
      case DoubleType           => JsNumber(k.asInstanceOf[Double])
      case FloatType            => JsNumber(k.asInstanceOf[Float])
      case ByteType             => JsNumber(k.asInstanceOf[Byte])
      case IntegerType          => JsNumber(k.asInstanceOf[Integer]:Int)
      case LongType             => JsNumber(k.asInstanceOf[Long])
      case ShortType            => JsNumber(k.asInstanceOf[Short])
      case NullType             => JsNull
      case _ if k != null       => JsString(k.toString)
      case _                    => JsNull
    }
  }
  def decode(k: JsValue, t: DataType): Any = 
  {
    t match {  
      case StringType           => k.as[String]
      case BinaryType           => base64Decode(k.as[String])
      case BooleanType          => k.as[Boolean]
      case DateType             => decodeDate(k.as[String])
      case TimestampType        => decodeTimestamp(k.as[String])
      case CalendarIntervalType => {
        val fields = k.as[Map[String,JsValue]]
        new CalendarInterval(fields("months").as[Int], fields("days").as[Int], fields("microseconds").as[Int])
      }
      case DoubleType           => k.as[Double]
      case FloatType            => k.as[Float]
      case ByteType             => k.as[Byte]
      case IntegerType          => k.as[Int]:Integer
      case LongType             => k.as[Long]
      case ShortType            => k.as[Short]
      case NullType             => JsNull
      case _                    => throw new IllegalArgumentException(s"Unsupported type for decode: $t")
    }
  }

  implicit def dataTypeFormat: Format[DataType] = Format(
    new Reads[DataType] { def reads(j: JsValue) = JsSuccess(DataType.fromJson(j.toString)) },
    new Writes[DataType] { def writes(t: DataType) = JsString(t.typeName) }
  )
}
