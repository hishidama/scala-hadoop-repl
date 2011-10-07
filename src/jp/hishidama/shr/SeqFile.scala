package jp.hishidama.shr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path => HPath, FileSystem, FileStatus }
import org.apache.hadoop.io.{ Writable, UTF8, Text }
import org.apache.hadoop.io.{ SequenceFile => HSeqFile }
import org.apache.hadoop.io.SequenceFile.{ Metadata, CompressionType }
import org.apache.hadoop.io.compress.{ CompressionCodec, DefaultCodec }
import java.io.Closeable
import org.apache.hadoop.io.NullWritable

class SeqFile(val path: Path, val conf: Configuration) {
  import SeqFile._

  private var _sequenceHeader = false
  private var _versionMatch = false
  private var _version = 0: Byte
  private var _keyClassName = ""
  private var _valClassName = ""
  private var _decompress = false
  private var _blockCompressed = false
  private var _codecClassname = ""
  private var _metadata: Metadata = null

  def isSequenceFile = _sequenceHeader && _versionMatch
  def sequenceHeader = _sequenceHeader
  def versionMatch = _versionMatch
  def version = _version
  def keyClassName = _keyClassName
  def valClassName = _valClassName
  def decompress = _decompress
  def blockCompressed = _blockCompressed
  def codecClassName = _codecClassname
  def metadata = _metadata

  lazy val keyClass: Class[_ <: Writable] = Class.forName(_keyClassName).asSubclass(classOf[Writable])
  lazy val valClass: Class[_ <: Writable] = Class.forName(_valClassName).asSubclass(classOf[Writable])

  //@see org.apache.hadoop.io.SequenceFile.Reader#init()
  using(path.fs.open(path.hpath)) { in =>
    val versionBlock = new Array[Byte](VERSION.length)
    in.readFully(versionBlock)

    if ((versionBlock(0) == VERSION(0)) &&
      (versionBlock(1) == VERSION(1)) &&
      (versionBlock(2) == VERSION(2))) {
      _sequenceHeader = true

      // Set 'version'
      _version = versionBlock(3)
      if (_version <= VERSION(3)) {
        _versionMatch = true

        if (_version < BLOCK_COMPRESS_VERSION) {
          val className = new UTF8()

          className.readFields(in)
          _keyClassName = className.toString() // key class name

          className.readFields(in)
          _valClassName = className.toString() // val class name
        } else {
          _keyClassName = Text.readString(in)
          _valClassName = Text.readString(in)
        }

        _decompress = if (_version > 2) { // if version > 2
          in.readBoolean() // is compressed?
        } else {
          false
        }

        _blockCompressed = if (_version >= BLOCK_COMPRESS_VERSION) { // if version >= 4
          in.readBoolean() // is block-compressed?
        } else {
          false
        }

        // if version >= 5
        // setup the compression codec
        if (_decompress) {
          _codecClassname = if (_version >= CUSTOM_COMPRESS_VERSION) {
            Text.readString(in)
          } else {
            "(default)"
          }
        }

        _metadata = new Metadata()
        if (_version >= VERSION_WITH_METADATA) { // if version >= 6
          _metadata.readFields(in)
        }
      }
    }
  }

  def show: Unit = cat
  def cat: Unit = {
    using(lines()) { r =>
      r.foreach(println)
    }
  }
  def head: Unit = head(Path.HEAD_DEFAULT_SIZE)
  def head(size: Int = Path.HEAD_DEFAULT_SIZE): Unit = {
    using(lines()) { r =>
      r.take(size).foreach(println)
    }
  }
  def tail: Unit = tail(Path.TAIL_DEFAULT_SIZE)
  def tail(size: Int = Path.TAIL_DEFAULT_SIZE): Unit = {
    using(lines()) { r =>
      r.toIterable.takeRight(size).foreach(println)
    }
  }
  def lines[K <: Writable, V <: Writable](): Iterator[(K, V)] with Closeable = {
    lines(
      getCreator(keyClass.asInstanceOf[Class[K]])(),
      getCreator(valClass.asInstanceOf[Class[V]])())
  }
  def lines[K <: Writable, V <: Writable](keyCreator: => K, valCreator: => V): Iterator[(K, V)] with Closeable = {
    val reader = openReader()
    def read(): (Boolean, K, V) = {
      val k = keyCreator
      val v = valCreator
      val b = reader.next(k, v)
      if (!b) { reader.close() }
      (b, k, v)
    }
    class ReaderIterator extends Iterator[(K, V)] with Closeable {
      private var t = read()
      def hasNext = t._1
      def next: (K, V) = {
        val r = (t._2, t._3)
        t = read()
        r
      }
      def close() = reader.close()
    }
    new ReaderIterator()
  }
  def openReader(): HSeqFile.Reader = new HSeqFile.Reader(path.fs(conf), path, conf)

  def getCreator[A](c: Class[A]): () => A = {
    c match {
      case c if c == classOf[NullWritable] =>
        () => { NullWritable.get.asInstanceOf[A] }
      case c =>
        () => { c.newInstance() }
    }
  }

  def describe: Seq[(String, String)] = Seq(
    ("path", path.toString()),
    ("isSequenceFile", isSequenceFile.toString()),
    ("version", version.toString()),
    ("keyClassName", keyClassName),
    ("valClassName", valClassName),
    ("decompress", decompress.toString()),
    ("blockCompressed", blockCompressed.toString()),
    ("codecClassName", codecClassName),
    ("metadata", metadata.toString()))

  override def toString(): String = {
    "SeqFile(" +
      path + "," +
      keyClassName + "," +
      valClassName +
      ")"
  }
}

object SeqFile {
  def apply(file: Path) = new SeqFile(file, conf)
  def apply(file: Path, c: Configuration) = new SeqFile(file, c)

  val BLOCK_COMPRESS_VERSION = 4: Byte
  val CUSTOM_COMPRESS_VERSION = 5: Byte
  val VERSION_WITH_METADATA = 6: Byte
  val VERSION = Array[Byte]('S', 'E', 'Q', VERSION_WITH_METADATA)

  def createWriter[K <: Writable, V <: Writable](file: Path)(implicit km: ClassManifest[K], vm: ClassManifest[V]): Writer[K, V] =
    createWriter(file, Path.conf)
  def createWriter[K <: Writable, V <: Writable](file: Path, conf: Configuration)(implicit km: ClassManifest[K], vm: ClassManifest[V]): Writer[K, V] =
    createWriter(file, km.erasure.asInstanceOf[Class[K]], vm.erasure.asInstanceOf[Class[V]], conf)
  def createWriter[K <: Writable, V <: Writable](
    file: Path,
    keyClass: Class[K],
    valClass: Class[V],
    conf: Configuration = Path.conf,
    compressionType: CompressionType = CompressionType.NONE,
    codec: CompressionCodec = new DefaultCodec()) = {
    new Writer[K, V](file, HSeqFile.createWriter(file.fs(conf), conf, file, keyClass, valClass, compressionType, codec))
  }

  class Writer[K <: Writable, V <: Writable](val path: Path, val hwriter: HSeqFile.Writer) extends Closeable {
    def keyClass = hwriter.getKeyClass()
    def valClass = hwriter.getValueClass()

    def append(key: K, value: V) = hwriter.append(key, value)
    override def close() = hwriter.close()

    override def toString(): String = {
      "SeqFile$Writer(" +
        path + "," +
        keyClass.getName() + "," +
        valClass.getName() +
        ")"
    }
  }
}
