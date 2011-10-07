package jp.hishidama.shr

import org.apache.hadoop.io.LongWritable

trait ImplicitWritableLong {

  implicit def writable2long(n: LongWritable) = n.get

  implicit def long2Writable(n: Long): LongWritable = {
    if (-128 <= n && n <= 127) {
      val i = n.toInt + 128
      var w = LongWritableCache(i)
      if (w == null) {
        w = new LongWritable(n) {
          private[this] val frozen = true
          override def set(n: Long) {
            if (frozen) throw new UnsupportedOperationException()
            super.set(n)
          }
        }
        LongWritableCache(i) = w
      }
      w
    } else {
      new LongWritable(n)
    }
  }
  private val LongWritableCache = new Array[LongWritable](256)

  implicit def longWritableOpt(w: LongWritable) = new LongWritableOpt(w)
  class LongWritableOpt(w: LongWritable) {
    def +=(n: Long) = w.set(w.get() + n)
    def -=(n: Long) = w.set(w.get() - n)
  }
}
