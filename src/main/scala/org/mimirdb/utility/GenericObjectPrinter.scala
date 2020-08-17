package org.mimirdb.utility

import scala.reflect.runtime.{universe => ru}
import scala.collection.mutable.Set

import java.util.UUID

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees._

import org.apache.spark.sql.catalyst.ScalaReflection._

import scala.reflect.ClassTag

import org.apache.commons.lang3.ClassUtils
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, FunctionResource}
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel


object GenericObjectPrinter {

  def pprint(obj: Any, depth: Int = 0, paramName: Option[String] = None): Unit = {

    val indent = "  " * depth
    val prettyName = paramName.fold("")(x => s"$x: ")

    if (obj == null) {
      println(s"$indent${prettyName}null")
    }
    else {
    val ptype = obj match { case _: Iterable[Any] => "" case obj: Product => obj.productPrefix case _ => obj.toString }

    println(s"$indent$prettyName$ptype")

    obj match {
      case seq: Iterable[Any] =>
        seq.foreach(pprint(_, depth + 1))
      case obj: Product =>
        (obj.productIterator zip Range(0,obj.productArity).iterator)
          .foreach { case (subObj, paramPos) => pprint(subObj, depth + 1, Some("Attr" + paramPos)) }
      case _ =>
    }
    }
  }

  def reflectiveToString(o: Object): String = {
    val str =  new StringBuilder()
    internalToString(o, str, new scala.collection.mutable.HashSet[Long]())
    str.toString()
  }

  def internalToString(o: Object, str: StringBuilder, haveSeen: Set[Long], verbose: Boolean = true) = o match {
    case Int | Double | Boolean | Float | Long | Short => str.append(o.toString())
    case x:java.lang.Long => str.append(o.toString())
    case x:java.lang.Integer => str.append(o.toString())
    case x:java.lang.Short => str.append(o.toString())
    case x:java.lang.Boolean => str.append(o.toString())
    case x:java.lang.Float => str.append(o.toString())
    case x:java.lang.Double => str.append(o.toString())
    case x:java.lang.String => str.append(o.toString())
    case _ => objectToString(o, str, haveSeen)
  }

  def objectToString(o: Object, str: StringBuilder, haveSeen: Set[Long], verbose: Boolean = true): Unit = {
    val id = System.identityHashCode(o)
    println("ID: " + id)
    if (haveSeen.contains(id))
    {
      str.append(s"-> $id")
    }
    else
    {
      val m = ru.runtimeMirror(o.getClass.getClassLoader)
      val im = m.reflect(o)
      val clazz = o.getClass()
      val fields = clazz.getDeclaredFields()
      val lastField = fields.last

      println("CLASS: " + clazz)
      haveSeen + id
      str.append(clazz.toString + "{")
      fields.map{
        f =>
        {
          println(f.getName())
          f.setAccessible(true)
          val mods = f.getModifiers()
          println(mods)
          if(!(java.lang.reflect.Modifier.isPrivate(mods) && java.lang.reflect.Modifier.isFinal(mods)))
          {
            val value = f.get(o)
            str.append(f.getName() + ": ")
            if (value == null) {
              str.append("null")
            }
            else {
              internalToString(value, str, haveSeen, verbose)
            }
            if(f != lastField)
              str.append(", ")
          }
          f.setAccessible(false)
        }
      }
      str.append("}\n")
    }
  }

}

object SparkTreePrinter {

  def toPrettyJSON[T <: TreeNode[_]](o: T): String = {
    pretty(render(SparkTreePrinter.jsonValue(o)))
  }

  def toJSON[T <: TreeNode[_]](o: T): JValue = {
    SparkTreePrinter.jsonValue(o)
  }

  private def jsonValue[T <: TreeNode[_]](o: T): JValue = {
    collectJsonValue(o)
  }

  private def collectJsonValue[T <: TreeNode[_]](
    tn: T
  ) : JValue = {
    val childvals = JArray(tn.children.toList.map( x => collectJsonValue(x.asInstanceOf[T])))
    val jsonFields = ("class" -> JString(tn.getClass.getName)) ::
    ("object ID" -> JString("" + System.identityHashCode(tn))) ::
    ("num-children" -> JInt(tn.children.length)) ::
    ("children:" -> childvals) ::
    SparkTreePrinter.jsonFields(tn) //
    JObject(jsonFields)
  }


  protected def jsonFields[T <: TreeNode[_]](o: T): List[JField] = {
    val fieldNames = getConstructorParameterNames(o.getClass)
    val fieldValues = o.productIterator.toSeq
    // assert(fieldNames.length == fieldValues.length, s"${o.getClass.getSimpleName} fields: " +
    //   fieldNames.mkString(", ") + s", values: " + fieldValues.mkString(", "))

    fieldNames.zip(fieldValues).map {
      // If the field value is a child, then use an int to encode it, represents the index of
      // this child in all children.
      case (name, value: TreeNode[_]) if o.containsChild(value) =>
        name -> JInt(o.children.indexOf(value))
      case (name, value: Seq[_]) if value.asInstanceOf[Seq[T]].forall(o.containsChild) =>
        name -> JArray(
          value.asInstanceOf[Seq[T]].map(v => JInt(o.children.indexOf(v.asInstanceOf[TreeNode[_]]))).toList
        )
      case (name, value) => name -> parseToJson(value)
    }.toList
  }

  private def parseToJson(obj: Any): JValue = obj match {
    case b: Boolean => JBool(b)
    case b: Byte => JInt(b.toInt)
    case s: Short => JInt(s.toInt)
    case i: Int => JInt(i)
    case l: Long => JInt(l)
    case f: Float => JDouble(f)
    case d: Double => JDouble(d)
    case b: BigInt => JInt(b)
    case null => JNull
    case s: String => JString(s)
    case u: UUID => JString(u.toString)
    case dt: DataType => JString(dt.typeName)
    // SPARK-17356: In usage of mllib, Metadata may store a huge vector of data, transforming
    // it to JSON may trigger OutOfMemoryError.
    case clazz: Class[_] => JString(clazz.getName)
    case n: TreeNode[_] => jsonValue(n)
    case o: Option[_] => o.map(parseToJson)
    // Recursive scan Seq[TreeNode], Seq[Partitioning], Seq[DataType]
    case t: Seq[_] if t.forall(_.isInstanceOf[TreeNode[_]]) || t.forall(_.isInstanceOf[DataType]) =>
      JArray(t.map(parseToJson).toList)
    case t: Seq[_] if t.length > 0 && t.head.isInstanceOf[String] =>
      JString(truncatedString(t, "[", ", ", "]", SQLConf.get.maxToStringFields))
    case t: Seq[_] => JNull
    case m: Map[_, _] => JNull
    // if it's a scala object, we can simply keep the full class path.
    // TODO: currently if the class name ends with "$", we think it's a scala object, there is
    // probably a better way to check it.
    case obj if obj.getClass.getName.endsWith("$") => "object" -> obj.getClass.getName
    case p: Product if shouldConvertToJson(p) =>
      try {
        val fieldNames = getConstructorParameterNames(p.getClass)
        val fieldValues = p.productIterator.toSeq
        assert(fieldNames.length == fieldValues.length, s"${getClass.getSimpleName} fields: " +
          fieldNames.mkString(", ") + s", values: " + fieldValues.mkString(", "))
        ("product-class" -> JString(p.getClass.getName)) :: fieldNames.zip(fieldValues).map {
          case (name, value) => name -> parseToJson(value)
        }.toList
      } catch {
        case _: RuntimeException => null
      }
    case _ => JNull
  }

  /**
   * Format a sequence with semantics similar to calling .mkString(). Any elements beyond
   * maxNumToStringFields will be dropped and replaced by a "... N more fields" placeholder.
   *
   * @return the trimmed and formatted string.
   */
  def truncatedString[T](
      seq: Seq[T],
      start: String,
      sep: String,
      end: String,
      maxFields: Int): String = {
    if (seq.length > maxFields) {
      val numFields = math.max(0, maxFields - 1)
      seq.take(numFields).mkString(
        start, sep, sep + "... " + (seq.length - numFields) + " more fields" + end)
    } else {
      seq.mkString(start, sep, end)
    }
  }


  private def shouldConvertToJson(product: Product): Boolean = product match {
    case exprId: ExprId => true
    case field: StructField => true
    case join: JoinType => true
    case spec: BucketSpec => true
    case catalog: CatalogTable => true
    case partition: Partitioning => true
    case resource: FunctionResource => true
    case broadcast: BroadcastMode => true
    case table: CatalogTableType => true
    case storage: CatalogStorageFormat => true
    case _ => false
  }


}
