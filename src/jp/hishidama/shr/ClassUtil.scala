package jp.hishidama.shr

import java.io.{ File => JFile }
import java.net.URL
import java.util.jar.JarFile

object ClassUtil {
  import scala.collection.JavaConverters._

  def findContainingJar(c: Class[_]): Seq[URL] = {
    //@see org.apache.hadoop.mapred.JobConf#findContainingJar()
    val loader = c.getClassLoader()
    val fname = classFileName(c)
    loader.getResources(fname).asScala.toSeq
  }

  def findFromFileSystem(className: String, dir: Path): Seq[JFile] = {
    val f = new JFile(dir.toUri())
    findFromFileSystem(className, f)
  }
  def findFromFileSystem(className: String, dir: JFile): Seq[JFile] = {
    val fname = classFileName(className)
    def find(f: JFile): Seq[JFile] = {
      if (f.isDirectory()) {
        val fs = f.listFiles()
        if (fs ne null) fs.map(find(_)).fold(Seq.empty)(_ ++ _)
        else Seq()
      } else {
        if (existsInJar(fname, f)) Seq(f)
        else Seq()
      }
    }
    find(dir)
  }

  def classFileName(c: Class[_]): String = {
    classFileName(c.getName())
  }
  def classFileName(className: String): String = {
    className.replace('.', '/') + ".class"
  }

  def existsInJar(fname: String, file: JFile): Boolean = {
    if (file.isFile() && file.getName().endsWith(".jar")) try {
      val jf = new JarFile(file)
      jf.getEntry(fname) ne null
    } catch { case _ => false }
    else false
  }
}
