package com.cj.collections

import java.util.concurrent.ConcurrentLinkedQueue

class IterableBlockingQueue[T] extends java.lang.Iterable[T] {
  private val queue: java.util.Queue[T] = new ConcurrentLinkedQueue[T]
  private var isDone: Boolean = false

  def done() {
    isDone = true
  }

  def add(`object`: T) {
    queue.add(`object`)
  }

  def size(): Int = queue.size

  def iterator(): java.util.Iterator[T] = new java.util.Iterator[T]() {
    def hasNext: Boolean = {
      try
        //TODO: Waiting 300ms is a naive solution to blocking.
        while (queue.isEmpty && !isDone) {Thread.sleep(300) }

      catch {
        case e: InterruptedException => {
        }
      }
      !(queue.isEmpty && isDone)
    }

    def next: T =
      return queue.remove

    override

    def remove() {
      queue.remove
    }
  }
}
