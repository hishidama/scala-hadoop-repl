package jp.hishidama.shr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

trait ImplicitHdfs {

  def conf = Path.conf
  def fs = FileSystem.get(conf)
  def fs(c: Configuration) = FileSystem.get(c)
  def fs(u: java.net.URI) = FileSystem.get(u, conf)
  def fs(p: Path) = FileSystem.get(p.uri, conf)

  implicit def filesystemLike(fs: FileSystem) = new FileSystemLike(fs)

  class FileSystemLike(fs: FileSystem) {
    def conf = fs.getConf()
    def conf_=(c: Configuration) = fs.setConf(c)
  }
}
