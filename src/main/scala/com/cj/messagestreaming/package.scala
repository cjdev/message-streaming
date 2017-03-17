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

  trait Subscription[T] extends Streamable[Checkpointable[T]] {
    def mapWithCheckpointing(f: T => Unit): Unit
  }

  object Subscription {
    def apply[T](stream: Stream[Checkpointable[T]]) = StreamSubscription(stream)
  }

  case class StreamSubscription[T](stream: Stream[Checkpointable[T]])
    extends Subscription[T] {
    override def mapWithCheckpointing(f: T => Unit): Unit = {
      stream.foreach {
        case Checkpointable(data, callback) =>
          f(data)
          callback()
      }
    }
  }

  case class Checkpointable[+T](data: T, checkpointCallback: CheckpointCallback) {

    def map[U](f: T => U): Checkpointable[U] =
      Checkpointable(f(data), checkpointCallback)

    def flatMap[U](k: T => Checkpointable[U]): Checkpointable[U] =
      k(data) match {
        case Checkpointable(newData, newCallback) =>
          Checkpointable(newData, () => { checkpointCallback(); newCallback() })
      }

    def runCheckpointable: T = { checkpointCallback(); data }
  }

  object Checkpointable {

    def apply[T](data: T): Checkpointable[T] = point(data)

    def point[T](data: T): Checkpointable[T] = Checkpointable(data, () => {})
  }

  type CheckpointCallback = () => Unit

  trait Publication[-T, +R] extends (T => Future[R]) with Closable
}
