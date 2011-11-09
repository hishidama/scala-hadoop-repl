package jp.hishidama.shr.view

import scala.swing._
import jp.hishidama.shr._

class SeqFileViewer(sf: SeqFile, val lineSize: Int, val skipBytes: Long) extends PathViewer(sf.path) {
  override lazy val viewerName = "SeqFileViewer"

  header.addRow("view line size", lineSize: java.lang.Integer)
  header.addRow("skip[Byte]", skipBytes: java.lang.Long)
  header.addRow("keyClassName", sf.keyClassName)
  header.addRow("valClassName", sf.valClassName)

  override lazy val viewer = new ScrollPane(new TextArea {
    editable = false
    text = {
      val conv = sf.getPrinter
      using(sf.getLines(skipBytes)) { _.take(lineSize).map(conv).mkString("\n") }
    }
  })

  override def toString() = {
    viewerName + "(" + path + "," + lineSize + "," + skipBytes + ")"
  }
}

object SeqFileViewer {
  def show(sf: SeqFile, lineSize: Int, skipBytes: Long) = {
    def findAdd(getClass: => Class[_], className: String): Unit = try {
      getClass
    } catch {
      case _: ClassNotFoundException =>
        val s = ClassFinder(className).show()
        if (s.nonEmpty) {
          println("add ClassPath = " + s)
          conf.addClassPath(s)
        }
    }
    findAdd(sf.keyClass, sf.keyClassName)
    findAdd(sf.valClass, sf.valClassName)

    val v = new SeqFileViewer(sf, lineSize, skipBytes)
    v.open()
    v
  }
}
