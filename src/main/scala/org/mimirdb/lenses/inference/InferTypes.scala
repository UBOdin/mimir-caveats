package org.mimirdb.lenses.inference

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.{ DataFrame, Column, Row }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import org.mimirdb.spark.SchemaLookup

object InferTypes
  extends LazyLogging
{

  def simpleCastTest(t:DataType):(Column => Column) = 
    (col: Column) => not(isnull(col.cast(t)))

  def integralTest(t:DataType):(Column => Column) = 
    (col: Column) => not(col.rlike("[0-9]+\\.")).and(simpleCastTest(t)(col))

  // Types to detect,  comparative ranks, and detectors.
  // The comparative rank is used as a tiebreaker when multiple attributes
  // parse at the same rate.  In general, we prioritize shorter matches when
  // available.
  // The detector should take in an input expression, and return a Column that
  // evaluates to true on inputs that are valid casts
  val TYPES = Seq[(DataType, Int, Column => Column)](
    (BooleanType,          0,     simpleCastTest(BooleanType)          ),
    (CalendarIntervalType, 0,     simpleCastTest(CalendarIntervalType) ),
    (TimestampType,        20,    simpleCastTest(TimestampType)        ),
    (DateType,             0,     simpleCastTest(DateType)             ),
    (DoubleType,           80,    simpleCastTest(DoubleType)           ),
    (FloatType,            60,    simpleCastTest(FloatType)            ),
    (LongType,             40,    integralTest(LongType)               ),
    (IntegerType,          20,    integralTest(IntegerType)            ),
    (ShortType,            0,     integralTest(ShortType)              ),

    // Skip string, since it's the default
    // (StringType,           0),

    // Parameterized Types (e.g., with nesting)
    // (ArrayType,            0),
    // (MapType,              0),
    // (ObjectType,           0),
    // (StructType,           0),
    // (UserDefinedType,      0) 

    // Types that can cast from anything
    // (AbstractDataType,     0),
    // (BinaryType,           0),
    // (ByteType,             0),
    // (NullType,             0),
  )

  def stringColumns(df: DataFrame): Seq[String] =
    df.schema match {
      case StructType(fields) => 
        fields.filter { _.dataType.equals(StringType) }
              .map { _.name }
      case _ => Seq.empty
    }

  def apply(
    df:DataFrame, 
    cutoff: Double = 0.5,
    attributes: Seq[String] = null
  ): Seq[StructField] =
  {
    val detectedTypes = 
      Option(attributes)
        .getOrElse { stringColumns(df) }
        .par
        .map { col:String => 
          col -> inferColumn(df, col)
                    .filter { _._2 > cutoff }
                    .headOption
                    .map { _._1 }
                    .getOrElse { StringType }
        }
        .seq
        .toMap

    logger.debug(s"Detected: $detectedTypes")

    df.schema match { 
      case StructType(fields) =>
        fields.map { field => 
          detectedTypes.get(field.name)
                    ///// Wrap up the detected type nicely if we have it
                       .map { StructField(field.name, _) }
                    ///// Otherwise, leave the field as-is
                       .getOrElse { field }
        }
      case _ => throw new IllegalArgumentException("Type inference on a non-dataframe")
    }

  }

  def inferColumn(
    df:DataFrame,
    attribute: String
  ): Seq[(DataType, Double)] =
  {
    val typeLookupQuery =
      df.na.drop().select(
        count(lit(true)).as("col_rows") +:
        (TYPES.map { case (t,_,test)  => 
          sum(
            when(test(df("`"+attribute.replaceAll("`", "``")+"`")), 1)
              .otherwise(0)
          ).as("col_as_"+t.typeName)
        } ):_*
      )
    def typeLookups = typeLookupQuery.collect()(0)

    val maxCount = typeLookups.getLong(0)

    TYPES.zipWithIndex
         .map { case ((t, weight, _), idx) =>
            logger.trace(s"TYPE: $t -> ${typeLookups.getLong(idx+1)}")
            val count = typeLookups.getLong(idx+1)
            
            (t, typeLookups.getLong(idx+1), weight)
         }
         .filter { _._2 > 0 }
         .sortBy { x => (-x._2, x._3) }
         .map { case (t, score, _) => t -> score.toDouble / maxCount }
  }

  def testRow(row: Row):Seq[(String,DataType)] = {
    val evalTarget = InternalRow(row.toSeq)
    SchemaLookup.rowReferences(row)
                .map { case (name, ref) => 
                  TYPES.flatMap { case (t, weight, test) =>
                    if(test(new Column(ref)).expr.eval(evalTarget).asInstanceOf[Boolean]){
                      Some(t -> weight)
                    } else { None }
                  }.sortBy { _._2 }
                   .headOption
                   .map { name -> _._1 }
                   .getOrElse { name -> StringType }
                }
  }

}