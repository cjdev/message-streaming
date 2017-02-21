package com.cj.collections

import scala.collection.mutable
import scala.collection.JavaConverters._

class IterableBlockingMultiQueue[T] private(
                                             priority: Ordering[T], // the greater is first
                                             initialAdders: Int,
                                             bound: Int
                                           )
  extends java.lang.Iterable[T] with Streamable[T] {

  private var ready = false

  private val queueOrder: Ordering[IterableBlockingQueue[T]] = new Ordering[IterableBlockingQueue[T]] {
    override def compare(x: IterableBlockingQueue[T], y: IterableBlockingQueue[T]): Int = {
      (x.noMore(), y.noMore()) match {
        case (true, true) => 0
        case (false, true) => -1 // empty is greater than nonempty
        case (true, false) => 1
        case (false, false) =>  {
          priority.compare(x.element(), y.element())
        }
        //otherwise order is the same as the order of their heads
      }
    }
  }
  private val priorityQueue: mutable.PriorityQueue[IterableBlockingQueue[T]] =
    mutable.PriorityQueue[IterableBlockingQueue[T]]()(queueOrder)

  private val pendingAdders: mutable.Queue[IterableBlockingQueue[T]] = mutable.Queue()

  override def iterator(): java.util.Iterator[T] = new java.util.Iterator[T]() {

    override def next(): T = {
      val q: IterableBlockingQueue[T] = priorityQueue.dequeue()
      pendingAdders.enqueue(q)
      q.remove()
    }

    override def hasNext: Boolean = {
      while (!ready) { Thread.sleep(300) }
      val addersToInsert = pendingAdders.dequeueAll(_ => true).filter(_.hasMore())
      addersToInsert.foreach(priorityQueue.enqueue(_))
      priorityQueue.nonEmpty && priorityQueue.head.nonEmpty
    }
  }

  def newAdder(): Queue[T] = {
    val q = new IterableBlockingQueue[T](bound)
    pendingAdders.enqueue(q)
    if (pendingAdders.size >= initialAdders) { ready = true }
    q
  }

  override def stream(): Stream[T] = iterator().asScala.toStream
}

object IterableBlockingMultiQueue {

  def apply[T](priority: Ordering[T],
               initialAdders: Int = 1,
               bound: Int = 20
              ): IterableBlockingMultiQueue[T] =
    new IterableBlockingMultiQueue[T](priority, initialAdders, bound)
}
