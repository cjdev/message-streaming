package com.cj

import scala.concurrent.Future

package object messagestreaming {

  trait Queue[T] {
    def add(t: T): Unit
    def done(): Unit
  }

  trait Streamable[+T] {
    def stream: Stream[T]
  }

  trait Closable {
    def close(): Unit
  }

  sealed abstract class Subscription[+T] extends Streamable[Checkpointable[T]] {

    def mapWithCheckpointing(f: T => Unit): Unit =
      Subscription.mapWithCheckpointing(this)(f)

    def x[U >: T](that: Subscription[U]): Subscription[U] =
      Subscription.interlace[U](this, that)

    def map[U](f: T => U): Subscription[U] =
      Subscription.map(this)(f)
  }

  object Subscription {

    def apply[T](s: Stream[Checkpointable[T]]): Subscription[T] =
      new Subscription[T] {
        def stream: Stream[Checkpointable[T]] = s
      }

    def map[T, U](sub: Subscription[T])(f: T => U): Subscription[U] =
      apply(sub.stream.map(_.map(f)))

    def mapWithCheckpointing[T](sub: Subscription[T])(f: T => Unit): Unit =
      sub.stream.foreach { case Checkpointable(data, callback) => f(data); callback() }

    def interlace[T](left: Subscription[T], right: Subscription[T]): Subscription[T] =
      (left.stream.isEmpty, right.stream.isEmpty) match {
        case (true, true) => Subscription(Stream.empty)
        case (true, false) => right
        case (false, true) => left
        case (false, false) =>
          Subscription(left.stream.head +: right.stream.head +: interlace(
            Subscription(left.stream.tail),
            Subscription(right.stream.tail)
          ).stream)
      }
  }

  case class Checkpointable[+T](data: T, checkpointCallback: CheckpointCallback) {

    def map[U](f: T => U): Checkpointable[U] =
      Checkpointable(f(data), checkpointCallback)

    def flatMap[U](k: T => Checkpointable[U]): Checkpointable[U] =
      k(data) match {
        case Checkpointable(newData, newCallback) =>
          Checkpointable(newData, _ => { checkpointCallback(); newCallback() })
      }

    def runCheckpointable: T = { checkpointCallback(); data }
  }

  object Checkpointable {

    def apply[T](data: T): Checkpointable[T] = point(data)

    def point[T](data: T): Checkpointable[T] = Checkpointable(data, _ => {})
  }

  type CheckpointCallback = Unit => Unit

  trait Publication[-T, +R] extends (T => Future[R]) with Closable
}
