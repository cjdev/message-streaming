package com.cj.collections
import java.util


class IterableBlockingMultiQueue[T] private (
                                             ordering: Ordering[T],
                                             bound: Int
                                             )
  extends java.lang.Iterable[T] with Streamable[T] {
  val priorityQueue: AnyRef{
    def add(t: IterableBlockingQueue[T]): Unit
    def pop(): IterableBlockingQueue[T]
  } = ???


  val queueOrder: Ordering[IterableBlockingQueue[T]] = new Ordering[IterableBlockingQueue[T]] {
    override def compare(x: IterableBlockingQueue[T], y: IterableBlockingQueue[T]): Int = ???
    //negative if x < y
    //positive if x > y
    //zero if x == y
  }


  override def iterator(): util.Iterator[T] =  new java.util.Iterator[T]() {
    override def next(): T = ???
    // pop x from priorityqueue
    // t: head of x
    //check if tail of x is empty                   <--
    // if nonempty, add it back to priorityQueue    <-- block on the NEXT element
    //return t


    override def hasNext: Boolean = ???
    //check if priority queue has next element      <-- blocking should occur here instead
  }

  override def stream: Stream[T] =  new IteratorStream(iterator())

  trait Adder {
    def add(item: T): Unit
    def done(): Unit
  }

  def newAdder(): Adder = ???
    //create a new --BOUNDED-- iterableblocking queue
    //register it
    //return it
}

object IterableBlockingMultiQueue {
  def apply[T](ordering: Ordering[T]): IterableBlockingMultiQueue[T] = ???
  def apply[T](ordering: Ordering[T], bound: Int): IterableBlockingMultiQueue[T] = ???
}