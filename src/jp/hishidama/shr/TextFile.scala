package jp.hishidama.shr

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable

class TextFile(val path: Path, val conf: Configuration) extends Show[String] {

  def fs = path.fs
  def size = path.size

  protected override def getPrinter = (s: String) => s
  protected override def getLines(skipBytes: Long) = openReader(skipBytes = skipBytes)

  def openReader(encoding: String = "UTF-8", skipBytes: Long = 0): Iterator[String] with Closeable = {
    val is = fs.open(path)
    skipBytes match {
      case 0 =>
      case n if n > 0 => is.seek(n)
      case n if n < 0 => val s = size + n; if (s > 0) is.seek(s)
    }
    val br = try {
      new BufferedReader(new InputStreamReader(is, encoding))
    } catch {
      case e =>
        try { is.close() } catch { case _ => }
        throw e
    }
    def read(): String = {
      val r = br.readLine()
      if (r == null) { br.close() }
      r
    }
    class ReaderIterator extends Iterator[String] with Closeable {
      private var s = read()
      def hasNext: Boolean = s != null
      def next: String = {
        val r = s
        s = read()
        r
      }
      def close() = br.close()
    }
    new ReaderIterator()
  }

  def openWriter(encoding: String = "UTF-8"): BufferedWriter = {
    val os = openOutputStream()
    try {
      new BufferedWriter(new OutputStreamWriter(os, encoding))
    } catch {
      case e =>
        try { os.close() } catch { case _ => }
        throw e
    }
  }
  def openOutputStream() = fs.create(path)

  override def toString(): String = {
    "TextFile(" +
      path +
      ")"
  }
}

object TextFile {
  def apply(file: Path) = new TextFile(file, conf)
  def apply(file: Path, c: Configuration) = new TextFile(file, c)
}
