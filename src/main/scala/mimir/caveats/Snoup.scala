package mimir.caveats

import com.typesafe.scalalogging.LazyLogging

class Snoup(x: AnyRef)
  extends LazyLogging
{
  def field[T](field: scala.Symbol): T = 
  {
    val clazz = x.getClass
    val element = clazz.getDeclaredField(field.name)
    // element.setAccessible(true)
    return element.get().asInstanceOf[T]
  }
  def method[T](method: scala.Symbol)(_args: Any*): T = 
  {
    val args = _args.map(_.asInstanceOf[AnyRef])
    val clazz = x.getClass
    // for(m <- clazz.getDeclaredMethods()){
    //   println(m.getName)
    // }
    val element = clazz.getDeclaredMethod(method.name)
    // def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
    // val parents = _parents.takeWhile(_ != null).toList
    // val methods = parents.flatMap(_.getDeclaredMethods)
    // val element = methods.find(_.getName == method.name).getOrElse(throw new IllegalArgumentException("Method " + method.name + " not found"))
    element.setAccessible(true)
    logger.debug("=======================================")
    logger.debug(s"$element")
    logger.debug(s"$x")
    logger.debug(s"$args")
    logger.debug("=======================================")
    element.invoke(x, args : _*).asInstanceOf[T]
  }
}

object Snoup
{
  def apply(x: AnyRef) = new Snoup(x)
}