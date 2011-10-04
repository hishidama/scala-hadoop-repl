package jp.hishidama.shr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path => HPath, FileSystem, FileStatus }
import org.apache.hadoop.io.UTF8
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile.Metadata

import SequenceFileInfo._

class SequenceFileInfo(val file: Path) {

  private var _sequenceFile = false
  private var _versionMatch = false
  private var _version = 0: Byte
  private var _keyClassName = ""
  private var _valClassName = ""
  private var _decompress = false
  private var _blockCompressed = false
  private var _codecClassname = ""
  private var _metadata: Metadata = null

  def isValid = _sequenceFile && _versionMatch
  def sequenceFile = _sequenceFile
  def versionMatch = _versionMatch
  def version = _version
  def keyClassName = _keyClassName
  def valClassName = _valClassName
  def decompress = _decompress
  def blockCompressed = _blockCompressed
  def codecClassName = _codecClassname
  def metadata = _metadata

  //@see org.apache.hadoop.io.SequenceFile.Reader#init()
  using(file.fs.open(file.hpath)) { in =>
    val versionBlock = new Array[Byte](VERSION.length);
    in.readFully(versionBlock);

    if ((versionBlock(0) == VERSION(0)) &&
      (versionBlock(1) == VERSION(1)) &&
      (versionBlock(2) == VERSION(2))) {
      _sequenceFile = true

      // Set 'version'
      _version = versionBlock(3)
      if (_version <= VERSION(3)) {
        _versionMatch = true

        if (_version < BLOCK_COMPRESS_VERSION) {
          val className = new UTF8();

          className.readFields(in);
          _keyClassName = className.toString(); // key class name

          className.readFields(in);
          _valClassName = className.toString(); // val class name
        } else {
          _keyClassName = Text.readString(in);
          _valClassName = Text.readString(in);
        }

        _decompress = if (_version > 2) { // if version > 2
          in.readBoolean(); // is compressed?
        } else {
          false;
        }

        _blockCompressed = if (_version >= BLOCK_COMPRESS_VERSION) { // if version >= 4
          in.readBoolean(); // is block-compressed?
        } else {
          false;
        }

        // if version >= 5
        // setup the compression codec
        if (_decompress) {
          _codecClassname = if (_version >= CUSTOM_COMPRESS_VERSION) {
            Text.readString(in);
          } else {
            "default"
          }
        }

        _metadata = new Metadata();
        if (_version >= VERSION_WITH_METADATA) { // if version >= 6
          _metadata.readFields(in);
        }
      }
    }
  }
}

object SequenceFileInfo {
  def apply(file: Path) = new SequenceFileInfo(file)

  val BLOCK_COMPRESS_VERSION = 4: Byte;
  val CUSTOM_COMPRESS_VERSION = 5: Byte;
  val VERSION_WITH_METADATA = 6: Byte;
  val VERSION = Array[Byte]('S', 'E', 'Q', VERSION_WITH_METADATA)
}
