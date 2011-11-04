package jp.hishidama.shr

import java.io.{ File => JFile }
import java.net.URL
import java.util.jar.JarFile
import scala.collection.mutable.ListBuffer

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
    val seq = ListBuffer[JFile]()
    findFromFileSystem(className, dir, false, _ => {}, (f: JFile) => synchronized { seq += f })
    seq.toSeq
  }

  def findFromFileSystem(className: String, dir: JFile, isStop: => Boolean, progress: JFile => Any, found: JFile => Any) {
    val fname = classFileName(className)
    def find(f: JFile) {
      if (!isStop) {
        if (f.isDirectory()) {
          progress(f)
          val fs = f.listFiles()
          if (fs ne null) fs.foreach(find)
        } else {
          if (isJar(f)) {
            progress(f)
            if (existClass(fname, f)) {
              found(f)
            }
          }
        }
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
    if (isJar(file)) existClass(fname, file) else false
  }

  def isJar(f: JFile): Boolean = {
    f.isFile() && f.getName().endsWith(".jar")
  }

  def existClass(fname: String, f: JFile): Boolean = try {
    val jf = new JarFile(f)
    jf.getEntry(fname) ne null
  } catch {
    case _ => false
  }
}
