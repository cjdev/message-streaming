package com.cj.collections

import java.util.concurrent.ConcurrentLinkedQueue


class IterableBlockingQueue[T] extends java.lang.Iterable[T] with Queue[T] with Streamable[T] {
  private val queue: java.util.Queue[T] = new ConcurrentLinkedQueue[T]
  private var isDone: Boolean = false

  def done() {
    isDone = true
  }

  def add(o: T) {
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

  def isEmpty(): Boolean = { queue.isEmpty && isDone }

  def nonEmpty(): Boolean = { !isEmpty() }

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
      nonEmpty()
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
