package jp.hishidama.shr

import org.apache.hadoop.io.IntWritable

trait ImplicitWritableInt {

  implicit def writable2int(n: IntWritable) = n.get

  implicit def int2Writable(n: Int): IntWritable = {
    if (-128 <= n && n <= 127) {
      val i = n + 128
      var w = IntWritableCache(i)
      if (w == null) {
        w = new IntWritable(n) {
          private[this] val frozen = true
          override def set(n: Int) {
            if (frozen) throw new UnsupportedOperationException()
            super.set(n)
          }
        }
        IntWritableCache(i) = w
      }
      w
    } else {
      new IntWritable(n)
    }
  }
  private val IntWritableCache = new Array[IntWritable](256)

  implicit def intWritableOpt(w: IntWritable) = new IntWritableOpt(w)
  class IntWritableOpt(w: IntWritable) {
    def +=(n: Int) = w.set(w.get() + n)
    def -=(n: Int) = w.set(w.get() - n)
  }
}
