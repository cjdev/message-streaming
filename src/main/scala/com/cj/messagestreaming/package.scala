package com.cj

import scala.concurrent.Future

package object messagestreaming {

  trait Queue[T] {
    def add(t: T): Unit
    def done(): Unit
  }

  trait Closable {
    def close(): Unit
  }

  sealed abstract class Subscription[+T] extends Iterable[Checkpointable[T]] {

    def mapWithCheckpointing(f: T => Unit): Unit =
      Subscription.process(this)(f)

    def map[U](f: T => U): Subscription[U] =
      Subscription.map(this)(f)
  }

  object Subscription {

    def apply[T](s: Iterator[Checkpointable[T]]): Subscription[T] =
      new Subscription[T] {
        def iterator: Iterator[Checkpointable[T]] = s
      }

    def map[T, U](sub: Subscription[T])(f: T => U): Subscription[U] =
      apply(sub.iterator.map(_.map(f)))

    def process[T](sub: Subscription[T])(f: T => Unit): Unit =
      sub.iterator.foreach { case Checkpointable(data, callback) => f(data); callback() }

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
