package jp.hishidama.shr.view

import java.lang.{ Boolean => JBoolean }
import java.io.{ File => JFile }
import scala.collection.mutable.ListBuffer
import scala.swing._
import scala.swing.event.ButtonClicked
import jp.hishidama.shr._
import java.awt.Dimension

class ClassFinder(val className: String) {

  def show: Seq[JFile] = show()
  def show(dir: String = ClassFinder.dir): Seq[JFile] = {
    ClassFinder.dir = dir
    val d = Dialog.showInput(
      title = className + " not found in ClassLoader",
      message = className + "が見つかりません。\n検索する場合は探す場所を入力して下さい。",
      initial = dir)
    d match {
      case Some(dir) =>
        val selector = new ClassPathSelector(className)
        selector.open()
        try {
          val progress = new ClassFindProgress(className, selector)
          progress.open()
          try {
            ClassUtil.findFromFileSystem(className,
              new JFile(LocalPath(dir).toUri()),
              { !progress.visible },
              (f: JFile) => { progress.label = f.toString() },
              (f: JFile) => { selector.addRow(f) })
          } finally {
            progress.dispose()
          }
          while (selector.visible) { Thread.sleep(100) }
          val r = selector.classPath
          r match {
            case Seq() =>
              Dialog.showMessage(message = "クラスパスが見つかりませんでした")
              show(dir)
            case s =>
              s
          }
        } finally {
          selector.dispose()
        }
      case None =>
        Seq()
    }
  }
}

object ClassFinder {
  var dir = "/"
  def apply(className: String) = new ClassFinder(className)
}

class ClassFindProgress(val className: String, owner: Window) extends Dialog(owner) {
  title = "progress find " + className
  private val _label = new Label("")
  contents = _label

  size = new Dimension(256, 64)

  def label = _label.text
  def label_=(path: String) = synchronized { _label.text = path }
}

class ClassPathSelector(val className: String) extends Dialog {
  selfFrame =>
  title = "classpath of " + className

  val table = new Table {
    import javax.swing.table.DefaultTableModel
    super.model = new DefaultTableModel {
      override def getColumnClass(columnIndex: Int) = columnIndex match {
        case 0 => classOf[JBoolean]
        case _ => classOf[Object]
      }
    }
    override lazy val model = super.model.asInstanceOf[DefaultTableModel]
    model.addColumn("選択")
    model.addColumn("classpath")
    do { //列幅固定
      val c = peer.getColumnModel().getColumn(0)
      c.setMinWidth(32)
      c.setMaxWidth(32)
    } while (false)

    peer.setColumnSelectionAllowed(true) //列選択可能

    override protected def editor(row: Int, column: Int) = {
      if (viewToModelColumn(column) == 0) super.editor(row, column) else null
    }
  }

  private var _classpath: Seq[JFile] = Seq.empty
  def classPath = _classpath

  contents = new BoxPanel(Orientation.Vertical) {
    contents += new Label("ClassLoaderに含めるパスを選択してください。")
    contents += new ScrollPane(table)
    contents += new BoxPanel(Orientation.Horizontal) {
      contents += new Button("OK") {
        reactions += {
          case ButtonClicked(source) =>
            val seq = ListBuffer[JFile]()
            for (i <- 0 until table.model.getRowCount()) {
              if (table.model.getValueAt(i, 0).asInstanceOf[JBoolean]) {
                seq += table.model.getValueAt(i, 1).asInstanceOf[JFile]
              }
            }
            _classpath = seq.toSeq
            selfFrame.close()
        }
      }
      contents += new Button("Cancel") {
        reactions += {
          case ButtonClicked(source) =>
            _classpath = Seq.empty
            selfFrame.close()
        }
      }
    }
  }

  def addRow(f: JFile): Unit = synchronized {
    val size = table.model.getRowCount()
    val select: JBoolean = size == 0
    table.model.addRow(Array[Object](select, f))
  }
}
