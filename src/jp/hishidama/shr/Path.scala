package jp.hishidama.shr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path => HPath, FileSystem, FileStatus }

import java.io._
import java.lang.Comparable
import java.net.URI

class Path(val hpath: HPath) extends Comparable[Path] with Show[String] {
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

  def la = { listAll.foreach(println); this }
  def la(dir: String) = { listAll(dir).foreach(println); this }
  def ls = { list.foreach(println); this }
  def ls(dir: String) = { list(dir).foreach(println); this }
  def list: Seq[Path] = listAll.filterNot(_.name.startsWith("."))
  def list(dir: String): Seq[Path] = listAll(dir).filterNot(_.name.startsWith("."))
  def listAll: Seq[Path] = listStatus.map(fs => Path(fs.getPath()))
  def listAll(dir: String): Seq[Path] = globStatus(dir).map(fs => Path(fs.getPath()))

  protected override def getPrinter = (s: String) => println(s)
  protected override def getLines(skipBytes: Long) = lines(skipBytes)

  override def more: Unit = more()
  override def more(size: Int = Path.MORE_DEFAULT_SIZE, skipBytes: Long = 0) = asSeqFileOption.getOrElse(this).doMore(size, skipBytes)
  override def head: Unit = head()
  override def head(size: Int = Path.HEAD_DEFAULT_SIZE): Unit = asSeqFileOption.getOrElse(this).doHead(size)
  override def tail: Unit = tail()
  override def tail(size: Int = Path.TAIL_DEFAULT_SIZE, skipBytes: Long = 0): Unit = asSeqFileOption.getOrElse(this).doTail(size, skipBytes)

  def lines(skipBytes: Long = 0) = openReader(skipBytes = skipBytes)
  def openReader(encoding: String = "UTF-8", skipBytes: Long = 0): Iterator[String] with Closeable = {
    val is = fs.open(hpath)
    skipBytes match {
      case 0 =>
      case n if n > 0 => is.seek(n)
      case n if n < 0 => val s = size + n; if (s > 0) is.seek(s)
    }
    val br = try {
      new BufferedReader(new InputStreamReader(is, encoding))
    } catch {
      case e =>
        try { is.close() } catch { case _ => }
        throw e
    }
    def read(): String = {
      val r = br.readLine()
      if (r == null) { br.close() }
      r
    }
    class ReaderIterator extends Iterator[String] with Closeable {
      private var s = read()
      def hasNext: Boolean = s != null
      def next: String = {
        val r = s
        s = read()
        r
      }
      def close() = br.close()
    }
    new ReaderIterator()
  }

  def #<(s: String): Unit = {
    using(openWriter()) { bw =>
      bw.write(s)
      //bw.write('\n')
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
        using(dp.openOutputStream()) { os =>
          val buf = new Array[Byte](Path.BUFFER_SIZE)
          ps.foreach { p =>
            using(fs.open(p)) { is =>
              var len = 0
              do {
                len = is.read(buf)
                if (len > 0) os.write(buf, 0, len)
              } while (len > 0)
            }
          }
        }
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
  def child(name: String) = Path(hpath, name)

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

  def asSeqFile = SeqFile(this)
  def asSeqFileOption: Option[SeqFile] = {
    val sf = if (isFile) SeqFile(this) else null
    if ((sf ne null) && sf.isSequenceFile) Some(sf) else None
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

  var conf = new Configuration
  def fs = FileSystem.get(conf)
  def home = Path(fs.getHomeDirectory())
  def work = Path(fs.getWorkingDirectory())
  def work_=(w: Path) = fs.setWorkingDirectory(w.hpath)

  var BUFFER_SIZE = conf.getInt("io.file.buffer.size", 4096)
  var HEAD_DEFAULT_SIZE = 10
  var TAIL_DEFAULT_SIZE = 10
  var MORE_DEFAULT_SIZE = 100
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
