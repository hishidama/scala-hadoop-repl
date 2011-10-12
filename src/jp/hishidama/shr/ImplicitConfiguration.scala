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
      conf.getClassLoader() match {
        case ul: URLClassLoader =>
          val urls = (ul.getURLs() ++ add).distinct
          val pl = ul.getParent()
          conf.setClassLoader(new URLClassLoader(urls, pl))
        case pl =>
          val urls = add.toArray[URL]
          conf.setClassLoader(new URLClassLoader(urls, pl))
      }
    }
  }
}
