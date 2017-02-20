package com.cj.collections
import java.util

import scala.collection.mutable

class IterableBlockingMultiQueue[T] private (
                                             ordering: Ordering[T],
                                             bound: Int
                                             )
  extends java.lang.Iterable[T] with Streamable[T] {

  val queueOrder: Ordering[IterableBlockingQueue[T]] = new Ordering[IterableBlockingQueue[T]] {
    override def compare(x: IterableBlockingQueue[T], y: IterableBlockingQueue[T]): Int = {
      (x.isEmpty(), y.isEmpty()) match {
        case (true , true ) => 0
        case (false, true ) => -1 // empty is greater than nonempty
        case (true , false) => 1
        case (false, false) => ordering.compare(x.element(),y.element())
          //otherwise order is the same as the order of their heads
      }
    }
  }

  val priorityQueue: mutable.PriorityQueue[IterableBlockingQueue[T]] =
    mutable.PriorityQueue[IterableBlockingQueue[T]]()(queueOrder)

  val pendingAdders: mutable.Queue[IterableBlockingQueue[T]] = mutable.Queue()

  override def iterator(): util.Iterator[T] =  new java.util.Iterator[T]() {
    override def next(): T = {
      val q: IterableBlockingQueue[T] = priorityQueue.dequeue()
      pendingAdders.enqueue(q)
      q.remove()
    }

    override def hasNext: Boolean = {
      val addersToInsert = pendingAdders.dequeueAll(_ => true).filter(_.nonEmpty())
      addersToInsert.foreach(priorityQueue.enqueue(_))
      priorityQueue.nonEmpty
    }
  }

  override def stream(): Stream[T] =  new IteratorStream(iterator())

  def newAdder(): Queue[T] = {
    val q = new IterableBlockingQueue[T]()
    pendingAdders.enqueue(q)
    q
  }
}

object IterableBlockingMultiQueue {
  def apply[T](ordering: Ordering[T]): IterableBlockingMultiQueue[T] =
    new IterableBlockingMultiQueue[T](ordering, 20)
  def apply[T](ordering: Ordering[T], bound: Int): IterableBlockingMultiQueue[T] =
    new IterableBlockingMultiQueue[T](ordering, bound)
}