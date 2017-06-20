package com.cj.messagestreaming

import java.util.concurrent.ConcurrentLinkedQueue

class IterableBlockingQueue[T]
  extends java.lang.Iterable[T] with Queue[T] with Streamable[T] {

  private val queue: java.util.Queue[T] = new ConcurrentLinkedQueue[T]

  private var isDone: Boolean = false

  def size: Int = queue.size

  def done(): Unit = {
    isDone = true
  }

  def add(o: T): Unit = {
    queue.add(o)
  }

  def iterator: java.util.Iterator[T] = new java.util.Iterator[T] {

    override def hasNext: Boolean = {
      try
        while (queue.isEmpty && !isDone) {
          Thread.sleep(300)
        }
      catch {
        case e: InterruptedException =>
      }
      !(queue.isEmpty && isDone)
    }

    override def next: T = {
      queue.remove
    }

    override def remove(): Unit = queue.remove
  }

  override def stream: Stream[T] = iteratorStream(iterator)

  private def iteratorStream[A](i: java.util.Iterator[A]): Stream[A] =
    new Stream[A] {
      override lazy val head: A = { i.hasNext; i.next }
      override lazy val tail: Stream[A] = { head; iteratorStream(i) }
      override def tailDefined: Boolean = false
      override def isEmpty: Boolean = !i.hasNext
    }
}
