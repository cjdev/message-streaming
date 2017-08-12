package com.cj

package object messagestreaming {

  trait Queue[T] {
    def add(t: T): Unit
    def done(): Unit
  }

  trait Closable {
    def close(): Unit
  }

  type CheckpointCallback = Unit => Unit
}
