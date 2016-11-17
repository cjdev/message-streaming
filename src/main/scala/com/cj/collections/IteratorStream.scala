package com.cj.collections

class IteratorStream[T]( i : java.util.Iterator[T]) extends Stream[T]{
  override lazy val head = { println("Fetching Head For "+i); i.hasNext ; i.next }
  override lazy val tail = { head; new IteratorStream(i)}
  override def tailDefined : Boolean = false
  override def isEmpty : Boolean = !i.hasNext
}