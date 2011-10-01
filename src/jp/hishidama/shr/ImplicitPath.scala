package jp.hishidama.shr

import java.io._

import org.apache.hadoop.fs.{ Path => HPath }

trait ImplicitPath {
  implicit def path2hadoopPath(p: Path) = p.hpath
  implicit def hadoopPath2Path(p: HPath): Path = Path(p)

  implicit def string2path(s: String): Path = Path(s)
  implicit def path2String(p: Path) = p.toString
  implicit def stringLikePath(s: String) = new StringLike(s)

  implicit def path2URI(p: Path) = p.uri
  implicit def URI2Path(u: java.net.URI): Path = Path(u)

  implicit def fileLike(f: File) = new FileLike(f)

  class StringLike(s: String) {
    def toPath = Path(s)
    def toLocalPath = LocalPath(s)
    def #>(p: Path) = p #< s
  }

  class FileLike(f: File) {
    def toPath = Path(f.toURI())
  }
}
