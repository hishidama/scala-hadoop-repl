package jp.hishidama.shr.view

import scala.swing._
import org.apache.hadoop.fs.{ Path => HPath, FileStatus }
import jp.hishidama.swing.tree.{ LazyTree, LazyTreeNode }
import jp.hishidama.scala_swing.RowSortTable
import jp.hishidama.shr._
import java.text.SimpleDateFormat

class DirViewer(path: Path) extends PathViewer(path) {
  override lazy val viewerName = "DirViewer"

  lazy val filesTable = new FilesTable
  override lazy val viewer = new SplitPane(Orientation.Vertical) {
    leftComponent = new ScrollPane(new DirTree)
    rightComponent = new ScrollPane(filesTable)

    dividerSize = 4
    dividerLocation = 224
  }

  class DirTree extends Component with Scrollable.Wrapper {
    import javax.swing.JTree
    import javax.swing.tree._
    import javax.swing.event._

    override lazy val peer: JTree = {
      val s = path.status
      val root = new DirNode(s)
      root.addChildNodes()
      filesTable.reset(s)

      new LazyTree(root) with SuperMixin {
        addTreeSelectionListener(new TreeSelectionListener {
          def valueChanged(e: TreeSelectionEvent) = {
            val list = e.getPaths().zipWithIndex.filter { case (_, i) => e.isAddedPath(i) }.map(_._1)
            list.headOption.foreach { tp =>
              val node = tp.getLastPathComponent().asInstanceOf[DirNode]
              filesTable.reset(node.status)
            }
          }
        })
      }
    }
    protected def scrollablePeer = peer

    class DirNode(val status: FileStatus) extends LazyTreeNode {
      val name = status.getPath().getName()
      protected def addChildNodesImpl() = {
        val list = status.getPath().listStatus.sortBy(_.getPath().getName())
        list.foreach { s =>
          if (s.isDir()) this.add(new DirNode(s))
        }
      }
      override def toString() = name
    }
  }

  class FilesTable extends RowSortTable {
    autoResizeMode = Table.AutoResizeMode.Off

    model.addColumn("No", classOf[java.lang.Integer])
    model.addColumn("name", classOf[String])
    model.addColumn("size", classOf[java.lang.Long])
    model.addColumn("replication", classOf[java.lang.Short])
    model.addColumn("modify", classOf[java.lang.String])

    val columnModel = peer.getColumnModel()
    columnModel.getColumn(0).setPreferredWidth(32)
    columnModel.getColumn(1).setPreferredWidth(120)
    columnModel.getColumn(3).setPreferredWidth(32)
    columnModel.getColumn(4).setPreferredWidth(128)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    def addRow(fs: FileStatus) = model.addRow(Array[AnyRef](
      (model.getRowCount() + 1): java.lang.Integer,
      fs.getPath().getName(),
      fs.getLen(): java.lang.Long,
      fs.getReplication(): java.lang.Short,
      sdf.format(new java.util.Date(fs.getModificationTime()))))
    def removeRows() = {
      while (model.getRowCount() > 0) {
        model.removeRow(model.getRowCount() - 1)
      }
    }

    def reset(fs: FileStatus) = {
      removeRows()
      val list = fs.getPath().listStatus.filterNot(_.isDir).sortBy(_.getPath().getName())
      list.foreach(addRow)
    }
  }
}

object DirViewer {
  def show(path: Path) = {
    val v = new DirViewer(path)
    v.visible = true
    v
  }
}
