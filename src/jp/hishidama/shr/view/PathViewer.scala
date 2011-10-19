package jp.hishidama.shr.view

import scala.swing._
import jp.hishidama.scala_swing.RowSortTable
import jp.hishidama.shr._

abstract class PathViewer(val path: Path) extends Frame {
  def viewerName: String
  title = viewerName + " " + path

  val header = new RowSortTable {
    model.addColumn("No", classOf[java.lang.Integer])
    model.addColumn("property", classOf[String])
    model.addColumn("value", classOf[Object])
    do {
      val c = peer.getColumnModel().getColumn(0)
      c.setMinWidth(32)
      c.setMaxWidth(32)
    } while (false)

    addRow("path", path)
    def addRow(name: String, value: AnyRef) = {
      val n = model.getRowCount() + 1
      model.addRow(Array[AnyRef](n: java.lang.Integer, name, value))
    }
  }

  def viewer: Component

  val split = new SplitPane(Orientation.Horizontal) {
    topComponent = new ScrollPane(header)
    bottomComponent = viewer

    dividerSize = 4
    dividerLocation = 100
  }
  contents = split
  size = new Dimension(640, 480)

  override def closeOperation() = dispose()

  override def toString() = {
    viewerName + "(" + path + ")"
  }
}
