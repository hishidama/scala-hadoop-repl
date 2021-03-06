package jp.hishidama.shr

import java.io.Closeable

trait Show[A] {
  protected def getPrinter: A => String
  protected def getLines(skipBytes: Long): Iterator[A] with Closeable

  def show: Unit = show()
  def show(skipBytes: Long = 0): Unit = more(skipBytes = skipBytes)
  def more: Unit = more()
  def more(size: Int = Path.MORE_DEFAULT_SIZE, skipBytes: Long = 0) = doMore(size, skipBytes)
  def cat: Unit = head()
  def head: Unit = head()
  def head(size: Int = Path.HEAD_DEFAULT_SIZE): Unit = doHead(size)
  def tail: Unit = tail()
  def tail(size: Int = Path.TAIL_DEFAULT_SIZE, skipBytes: Long = 0): Unit = doTail(size, skipBytes)

  def lines: Iterator[A] with Closeable = lines()
  def lines(skipBytes: Long = 0): Iterator[A] with Closeable = getLines(skipBytes)

  protected def doMore(size: Int, skipBytes: Long): Unit = {
    using(getLines(skipBytes)) { r =>
      val conv = getPrinter

      import scala.util.control.Breaks.{ break, breakable }
      breakable {
        var i = 0
        var more1 = false
        r.foreach { a =>
          i += 1
          if (i > size || more1) {
            if (!more1) println("more?")
            val n = scala.Console.in.read
            if (n < 0) break
            n.toChar.toLower match {
              case 'q' => break
              case '\r' | '\n' =>
                more1 = true
              case _ =>
                more1 = false
                i = 1
            }
          }
          println(conv(a))
        }
      }
    }
  }

  protected def doHead(size: Int): Unit = {
    using(getLines(0)) { r =>
      val conv = getPrinter
      r.take(size).foreach(s => println(conv(s)))
    }
  }

  protected def doTail(size: Int, skipBytes: Long): Unit = {
    using(getLines(skipBytes)) { r =>
      val conv = getPrinter
      r.toIterable.takeRight(size).foreach(s => println(conv(s)))
    }
  }
}
