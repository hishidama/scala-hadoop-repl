package jp.hishidama.shr

import org.apache.hadoop.io._

trait ImplicitWritable {
  // Text
  implicit def text2String(t: Text) = t.toString
  implicit def string2text(s: String) = new Text(s)

  // Int
  implicit def writable2int(n: IntWritable) = n.get
  implicit def int2writable(n: Int) = new IntWritable(n)

  // Long
  implicit def writable2long(n: LongWritable) = n.get
  implicit def long2writable(n: Long) = new LongWritable(n)
}
