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
      message = className + "��������܂���B\n��������ꍇ�͒T���ꏊ����͂��ĉ������B",
      initial = dir)
    d match {
      case Some(dir) =>
        val r = ClassUtil.findFromFileSystem(className, LocalPath(dir))
        r match {
          case Seq() =>
            Dialog.showMessage(message = "������܂���ł���")
            show(dir)
          case s @ Seq(_) => //1�t�@�C���������������炻���Ԃ�
            s
          case s =>
            s //TODO �I�����ĕԂ�
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
