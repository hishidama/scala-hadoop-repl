package jp.hishidama

import java.io.Closeable

package object shr extends ImplicitHdfs
  with ImplicitPath {

  def using[A <: Closeable, B](r: A)(f: A => B): B = {
    try f(r) finally r.close()
  }
}
