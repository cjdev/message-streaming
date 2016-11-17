package com.cj.collections

import java.util.concurrent.ConcurrentLinkedQueue

class IterableBlockingQueue[T] extends java.lang.Iterable[T] with Queue[T] {
//  private val queue: java.util.Queue[(T, Unit=>Unit)] = new ConcurrentLinkedQueue[(T, Unit=>Unit)]
  private val queue: java.util.Queue[T] = new ConcurrentLinkedQueue[T]
  private var isDone: Boolean = false
//  private var nextCallback: Unit => Unit = (_: Unit) => {}

  def done() {
    isDone = true
  }

  def add(o: T) {
//    this.add(`object`, _ => {})
    queue.add(o)
  }

//  def add(`object`: T, callback: Unit=>Unit) {
//    queue.add(`object`, callback)
//  }

  def size(): Int = queue.size

  def iterator(): java.util.Iterator[T] = new java.util.Iterator[T]() {
    override def hasNext: Boolean = {
//      nextCallbackOnce()
      try
        //TODO: Waiting 300ms is a naive solution to blocking.
        while (queue.isEmpty && !isDone) { Thread.sleep(300) }

      catch {
        case e: InterruptedException => {
        }
      }
      !(queue.isEmpty && isDone)
    }
//
//    def nextCallbackOnce(): Unit = {
//      nextCallback()
//      nextCallback = _ => {}
//    }

    override def next: T = {
//      nextCallbackOnce()
//      val (thing, func) = queue.remove
//      nextCallback = func
//      thing
      queue.remove
    }

    override def remove() {
      queue.remove
    }
  }
}
