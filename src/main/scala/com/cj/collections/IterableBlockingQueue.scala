package com.cj.collections

import java.util.concurrent.ConcurrentLinkedQueue


class IterableBlockingQueue[T](bound : Int = Int.MaxValue) extends java.lang.Iterable[T] with Queue[T] with Streamable[T] {
  private val queue: java.util.Queue[T] = new ConcurrentLinkedQueue[T]
  private var isDone: Boolean = false

  def done() {
    isDone = true
  }

  def add(o: T) {
    while (size >= bound) {
      Thread.sleep(300)
    }
    queue.add(o)
  }

  def remove(): T = {
    waitForElement()
    queue.remove()
  }

  def element(): T = {
    waitForElement()
    queue.element()
  }

  def noMore(): Boolean = { queue.isEmpty && isDone }

  def hasMore(): Boolean = { !noMore() }

  def waitForElement(): Unit = {
    try
      //TODO: Waiting 300ms is a naive solution to blocking.
      while (queue.isEmpty && !isDone) { Thread.sleep(300) }
    catch {
      case e: InterruptedException =>
    }
  }

  def size(): Int = queue.size

  def iterator(): java.util.Iterator[T] = new java.util.Iterator[T]() {
    override def hasNext: Boolean = {
      waitForElement()
      hasMore()
    }

    override def next: T = {
      queue.remove()
    }

    override def remove() {
      queue.remove()
    }
  }
  
  override def stream():Stream[T] = new IteratorStream(iterator()) 
}
