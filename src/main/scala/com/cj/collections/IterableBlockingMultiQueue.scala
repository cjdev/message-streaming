package com.cj.collections

import scala.collection.mutable

class IterableBlockingMultiQueue[T] private(
                                             ordering: Ordering[T],
                                             bound: Int
                                           )
  extends java.lang.Iterable[T] with Streamable[T] {

  private val queueOrder: Ordering[IterableBlockingQueue[T]] = new Ordering[IterableBlockingQueue[T]] {
    override def compare(x: IterableBlockingQueue[T], y: IterableBlockingQueue[T]): Int = {
      (x.isEmpty(), y.isEmpty()) match {
        case (true, true) => 0
        case (false, true) => -1 // empty is greater than nonempty
        case (true, false) => 1
        case (false, false) => ordering.compare(x.element(), y.element())
        //otherwise order is the same as the order of their heads
      }
    }
  }

  // holds adders ready to be read from
  private val priorityQueue: mutable.PriorityQueue[IterableBlockingQueue[T]] =
    mutable.PriorityQueue[IterableBlockingQueue[T]]()(queueOrder)

  // holds adders waiting to be written to
  private val pendingAdders: mutable.Queue[IterableBlockingQueue[T]] = mutable.Queue()

  override def iterator(): java.util.Iterator[T] = new java.util.Iterator[T]() {

    override def next(): T = {
      // grab the top-most element from 'priorityQueue'
      val q: IterableBlockingQueue[T] = priorityQueue.dequeue()

      // put it on 'pendingAdders'
      pendingAdders.enqueue(q)

      // get its top-most element
      q.remove()
    }

    override def hasNext: Boolean = {
      // empty 'pendingAdders', and grab the non-empty elements
      val addersToInsert = pendingAdders.dequeueAll(_ => true).filter(_.nonEmpty())

      // take the non-empty formerly pending adders and put them on 'priorityQueue'
      addersToInsert.foreach(priorityQueue.enqueue(_))

      // check if 'priorityQueue' is non-empty
      priorityQueue.nonEmpty
    }
  }

  def newAdder(): Queue[T] = {
    // make a new empty queue
    val q = new IterableBlockingQueue[T]()

    // put it on 'pendingAdders'
    pendingAdders.enqueue(q)

    // hand it to the client
    q
  }

  override def stream(): Stream[T] = new IteratorStream(iterator())
}

object IterableBlockingMultiQueue {

  def apply[T](
                ordering: Ordering[T],
                bound: Int = 20
              ): IterableBlockingMultiQueue[T] =
    new IterableBlockingMultiQueue[T](ordering, bound)
}
