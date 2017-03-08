package com.cj

import scala.concurrent.Future

package object messagestreaming {

  trait Queue[T] {
    def add(t: T): Unit
    def done(): Unit
  }

  trait Streamable[T] {
    def stream: Stream[T]
  }

  trait Closable {
    def close(): Unit
  }

  case class IteratorStream[T](i: java.util.Iterator[T]) extends Stream[T] {
    override lazy val head: T = { i.hasNext; i.next }
    override lazy val tail: IteratorStream[T] = { head; IteratorStream(i)}
    override def tailDefined: Boolean = false
    override def isEmpty: Boolean = !i.hasNext
  }

  trait Subscription extends Streamable[CheckpointableRecord] {
    def mapWithCheckpointing(f: Array[Byte] => Unit): Unit
  }

  object Subscription {
    def apply(stream: Stream[CheckpointableRecord]) = StreamSubscription(stream)
  }

  case class StreamSubscription(stream: Stream[CheckpointableRecord])
    extends Subscription {
    override def mapWithCheckpointing(f: Array[Byte] => Unit): Unit = {
      stream.foreach {
        case CheckpointableRecord(data, callback) =>
          f(data)
          callback()
      }
    }
  }

  case class CheckpointableRecord(data: Array[Byte], checkpointCallback: CheckpointCallback)

  type CheckpointCallback = () => Unit

  trait Publication[-T, +R] extends (T => Future[R]) with Closable
}
