package com.cj

package object collections {
  implicit def steamableToStream[T](s: Streamable[T]): Stream[T] = s.stream
}