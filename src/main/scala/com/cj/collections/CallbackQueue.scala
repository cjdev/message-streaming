package com.cj.collections

trait CallbackQueue[T] {
  def add(`object`: T, callback: Unit=>Unit)
  def done()

}
