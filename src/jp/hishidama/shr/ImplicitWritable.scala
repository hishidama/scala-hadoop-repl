package jp.hishidama.shr

import org.apache.hadoop.io._

trait ImplicitWritable {
  // Text
  implicit def text2String(t: Text) = t.toString
  implicit def string2text(s: String) = new Text(s)
}
