package jp.hishidama.shr

import org.apache.hadoop.conf.Configuration
import java.io.{ File => JFile }
import java.net.{ URL, URLClassLoader }

trait ImplicitConfiguration {

  implicit def configurationLike(conf: Configuration) = new ConfigurationLike(conf)

  class ConfigurationLike(conf: Configuration) {
    def addClassPath(f: JFile): Unit = addClassPath(Seq(f))
    def addClassPath(cp: Seq[_]): Unit = {
      val add: Seq[URL] = cp map {
        _ match {
          case u: URL => u
          case f: JFile => f.toURI.toURL
          case s: String => new JFile(s).toURI.toURL
          case p: Path => p.toURL
        }
      }
      val parent = conf.getClassLoader()
      val olds = getURLs(parent)
      val urls = add.filterNot(olds.contains)
      if (urls.nonEmpty) {
        conf.setClassLoader(new URLClassLoader(urls.toArray, parent))
      }
    }

    private def getURLs(cl: ClassLoader): Set[URL] = {
      cl match {
        case ul: URLClassLoader =>
          getURLs(ul.getParent()) ++ ul.getURLs()
        case _ => Set()
      }
    }
  }
}
