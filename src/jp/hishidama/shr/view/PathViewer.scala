package jp.hishidama.shr.view

import scala.swing._
import jp.hishidama.shr._

abstract class PathViewer(val path: Path) extends Frame {
  def viewerName: String
  title = viewerName + " " + path

  val header = new Table {
    override lazy val model = super.model.asInstanceOf[javax.swing.table.DefaultTableModel]

    model.addColumn("property")
    model.addColumn("value")

    addRow("path", path)
    def addRow(name: String, value: AnyRef) = model.addRow(Array[AnyRef](name, value))
  }

  def viewer: Component

  val split = new SplitPane(Orientation.Horizontal) {
    topComponent = new ScrollPane(header)
    bottomComponent = new ScrollPane(viewer)

    dividerSize = 4
    dividerLocation = 64
  }
  contents = split
  size = new Dimension(640, 480)

  override def closeOperation() = dispose()

  override def toString() = {
    viewerName + "(" + path + ")"
  }
}
