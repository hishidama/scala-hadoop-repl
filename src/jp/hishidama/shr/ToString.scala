package jp.hishidama.shr
import java.lang.reflect.Method

trait ToString[A] extends (A => String)

object ToString {
  def apply[A](c: Class[A]): ToString[A] = {
    if (isDefault(c)) new ToStringGet(c)
    else new ToStringDefault[A]()
  }

  def isDefault[A](c: Class[A]): Boolean = {
    if (c == classOf[Object]) true else try {
      c.getDeclaredMethod("toString") //’è‹`‚³‚ê‚Ä‚¢‚È‚¢ê‡‚Í—áŠO”­¶
      false
    } catch { case _ => isDefault(c.getSuperclass()) }
  }

  def create[A](c: Class[A]): (A) => String = {
    val ms = c.getMethods()
    val gs = ms.flatMap { m =>
      nameFromGetter(m.getName()) match {
        case Some(n) => Some(n, m)
        case _ => None
      }
    }.filter {
      case (_, m) =>
        m.getParameterTypes().length == 0 && m.getReturnType() != Void.TYPE
    }

    val rs = gs.sortBy(_._1).map(_._2)
    val cname = c.getSimpleName()
    (a: A) => { rs.map { m => m.getName() + "=" + m.invoke(a) }.mkString(cname + "{", ", ", "}") }
  }

  def nameFromGetter(getterName: String): Option[String] = {
    getterName match {
      case "getClass" => None
      case "get" => None
      case "is" => None
      case n if n.startsWith("get") => Some(lowerHead(n.substring(3)))
      case n if n.startsWith("is") => Some(lowerHead(n.substring(2)))
      case _ => None
    }
  }

  def lowerHead(s: String): String = s.charAt(0).toLowerCase + s.substring(1)
}

class ToStringDefault[A] extends ToString[A] {
  def apply(a: A) = a.toString()
}

class ToStringGet[A](val clazz: Class[A]) extends ToString[A] {
  private val _toString: A => String = ToString.create(clazz)
  def apply(a: A): String = _toString(a)
}
