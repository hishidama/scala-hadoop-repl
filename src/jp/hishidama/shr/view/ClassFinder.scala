package jp.hishidama.shr.view

import java.io.{ File => JFile }
import scala.swing.Frame
import scala.swing.Dialog
import jp.hishidama.shr._

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
        val r = ClassUtil.findFromFileSystem(className, LocalPath(dir))
        r match {
          case Seq() =>
            Dialog.showMessage(message = "見つかりませんでした")
            show(dir)
          case s @ Seq(_) => //1ファイルだけ見つかったらそれを返す
            s
          case s =>
            s //TODO 選択して返す
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
