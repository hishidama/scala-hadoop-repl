package jp.hishidama.shr.view

import scala.swing._
import jp.hishidama.shr._

class TextViewer(path: Path, val lineSize: Int, val skipBytes: Long) extends PathViewer(path) {
  override lazy val viewerName = "TextViewer"

  header.addRow("view line size", lineSize: java.lang.Integer)
  header.addRow("skip[Byte]", skipBytes: java.lang.Long)

  override lazy val viewer = new TextArea {
    editable = false
    text = using(path.lines(skipBytes)) { _.take(lineSize).mkString("\n") }
  }

  override def toString() = {
    viewerName + "(" + path + "," + lineSize + "," + skipBytes + ")"
  }
}

object TextViewer {
  def show(path: Path, lineSize: Int, skipBytes: Long) = {
    val v = new TextViewer(path, lineSize, skipBytes)
    v.visible = true
    v
  }
}
