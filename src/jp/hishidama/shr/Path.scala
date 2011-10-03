package jp.hishidama.shr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path => HPath, FileSystem, FileStatus }

import java.io._
import java.lang.Comparable
import java.net.URI

class Path(val hpath: HPath) extends Comparable[Path] {
  self =>

  def parent = Path(hpath.getParent())
  def name = hpath.getName()
  def uri = hpath.toUri()

  def fs: FileSystem = fs(conf)
  def fs(c: Configuration) = /*if (isLocal) FileSystem.getLocal(c) else*/ hpath.getFileSystem(c)

  def isAbsolute = hpath.isAbsolute()
  def isDirectory = fs.isDirectory(hpath)
  def isFile = fs.isFile(hpath)
  def isLocal = uri.getScheme() == "file"
  def exists = fs.exists(hpath)
  def depth = hpath.depth()
  def size: Long = status.getLen()
  def status = fs.getFileStatus(hpath)

  override def equals(that: Any) = that match {
    case that: Path => hpath.equals(that.hpath)
    case that: HPath => hpath.equals(that)
    case _ => false
  }
  override def compareTo(that: Path) = hpath.compareTo(that.hpath)
  override def hashCode = hpath.hashCode()

  override def toString() = hpath.toString()

  def mkdirs() = fs.mkdirs(hpath)
  def rm = fs.delete(hpath)
  def rm(name: String) = {
    val fs = self.fs
    globStatus(name).foreach(f => fs.delete(f.getPath()))
  }

  def la = { list.foreach(println); this }
  def la(dir: String) = { list(dir).foreach(println); this }
  def ls = { list.filterNot(_.name.startsWith(".")).foreach(println); this }
  def ls(dir: String) = { list(dir).filterNot(_.name.startsWith(".")).foreach(println); this }
  def list: Seq[Path] = listStatus.map(fs => Path(fs.getPath()))
  def list(dir: String): Seq[Path] = globStatus(dir).map(fs => Path(fs.getPath()))

  def show: Unit = cat
  def cat: Unit = {
    val r = lines()
    try {
      r.foreach(println)
    } finally r.close()
  }
  def head(size: Int = 10): Unit = {
    val r = lines()
    try {
      r.take(size).foreach(println)
    } finally r.close()
  }
  def tail(size: Int = 10): Unit = {
    val r = lines()
    try {
      r.toIterable.takeRight(size).foreach(println)
    } finally r.close()
  }
  def lines() = openReader()
  def openReader(encoding: String = "UTF-8"): Iterator[String] with Closeable = {
    val is = fs.open(hpath)
    val br = try {
      new BufferedReader(new InputStreamReader(is, encoding))
    } catch {
      case e =>
        try { is.close() } catch { case _ => }
        throw e
    }
    new Iterator[String] with Closeable {
      var s = br.readLine()
      def hasNext: Boolean = s != null
      def next: String = {
        val r = s
        s = br.readLine()
        if (s == null) { close() }
        r
      }
      def close() = br.close()
    }
  }

  def #<(s: String): Unit = {
    val bw = openWriter()
    try {
      bw.write(s)
      //bw.write('\n')
    } finally {
      bw.close()
    }
  }
  def openWriter(encoding: String = "UTF-8"): BufferedWriter = {
    val os = openOutputStream()
    try {
      new BufferedWriter(new OutputStreamWriter(os, encoding))
    } catch {
      case e =>
        try { os.close() } catch { case _ => }
        throw e
    }
  }
  def openOutputStream() = fs.create(hpath)

  def copy(dst: Path) = {
    if (isLocal) {
      fs.copyFromLocalFile(hpath, dst.hpath)
    } else {
      fs.copyToLocalFile(hpath, dst.hpath)
    }
  }

  def merge(name: String, dst: Path): Int = {
    merge(list(name), dst)
  }
  def merge(ns: Seq[Path], dst: Path): Int = {
    ns.filter(_.isFile) match {
      case Seq() => 0
      case Seq(p) => p.copy(dst); 1
      case ps =>
        val dp = if (dst.isDirectory) Path(dst.hpath, ps.head.name) else dst
        val os = dp.openOutputStream()
        try {
          val buf = new Array[Byte](Path.BUFFER_SIZE)
          ps.foreach { p =>
            val is = fs.open(p)
            try {
              var len = 0
              do {
                len = is.read(buf)
                if (len > 0) os.write(buf, 0, len)
              } while (len > 0)
            } finally is.close()
          }
        } finally os.close()
        ps.size
    }
  }

  def cd(dir: String): Path = {
    resolve(dir) match {
      case d if d.isDirectory => d
      case f if f.isFile => throw new FileNotFoundException("not directory")
      case _ => throw new FileNotFoundException()
    }
  }
  def file(name: String): Path = {
    resolve(name) match {
      case d if d.isDirectory => throw new FileNotFoundException("not file")
      case f if f.isFile => f
      case _ => throw new FileNotFoundException()
    }
  }
  def resolve(name: String): Path = globStatus(name) match {
    case Nil => Path(hpath, name)
    case seq => Path(seq.head.getPath())
  }
  def rel(name: String) = Path(hpath, name)

  def listStatus: Seq[FileStatus] = fs.listStatus(hpath) match {
    case null => Seq()
    case r => r
  }
  def globStatus(name: String): Seq[FileStatus] = {
    val fs = self.fs
    def resolve(p: HPath, names: List[String]): Seq[FileStatus] = {
      names match {
        case Nil => Seq()
        case name :: Nil =>
          val r = fs.globStatus(new HPath(p, name))
          if (r != null) r else Seq()
        case name :: tail =>
          fs.globStatus(new HPath(p, name)).flatMap { s =>
            resolve(s.getPath(), tail)
          }
      }
    }
    resolve(hpath, name.split("/").toList)
  }
}

object Path {
  def apply(path: HPath) = new Path(path)
  def apply(path: String) = new Path(new HPath(path))
  def apply(parent: String, child: String) = new Path(new HPath(parent, child))
  def apply(parent: String, child: HPath) = new Path(new HPath(parent, child))
  def apply(parent: HPath, child: String) = new Path(new HPath(parent, child))
  def apply(parent: HPath, child: HPath) = new Path(new HPath(parent, child))
  def apply(uri: URI) = new Path(new HPath(uri))

  var BUFFER_SIZE = 64 * 1024

  var conf = new Configuration
  def fs = FileSystem.get(conf)
  def home = Path(fs.getHomeDirectory())
  def work = Path(fs.getWorkingDirectory())
  def work_=(w: Path) = fs.setWorkingDirectory(w.hpath)
}

trait LocalPath {
  def apply(path: String) = Path(toFile(path).toURI())

  import scala.sys.process.Process

  lazy val isCygwin: Boolean = {
    try {
      Process(Seq("cygpath", "-m", "/")).lines.nonEmpty
    } catch {
      case _ =>
        false
    }
  }

  def toFile(s: String): File = {
    val m = if (isCygwin) try {
      Process(Seq("cygpath", "-m", s)).lines.head
    } catch {
      case _ =>
        s.replaceAll("\\", "/")
    }
    else s
    new File(m)
  }

  def conf = Path.conf
  def fs = FileSystem.getLocal(conf)
  def home = Path(fs.getHomeDirectory())
  def work = Path(fs.getWorkingDirectory())
  def work_=(w: Path) = fs.setWorkingDirectory(w.hpath)
  def work_=(w: String) = fs.setWorkingDirectory(new HPath(toFile(w).toURI()))
}

object LocalPath extends LocalPath
object File extends LocalPath
