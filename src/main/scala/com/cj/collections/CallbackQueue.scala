package com.cj.collections

trait CallbackQueue[T] {
  def add(`object`: T)
  def done()

}
