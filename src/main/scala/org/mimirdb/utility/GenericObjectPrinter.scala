package org.mimirdb.utility

import scala.reflect.runtime.{universe => ru}
import scala.collection.mutable.Set

object GenericObjectPrinter {


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
