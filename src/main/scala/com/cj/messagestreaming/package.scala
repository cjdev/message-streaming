package com.cj

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

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

  trait Publication[-T, +R] extends (T => Future[R]) with Closable { self =>

    final def premap[T1](f: T1 => T): Publication[T1, R] =
      new Publication[T1, R] {
        def apply(v1: T1): Future[R] = self(f(v1))
        def close(): Unit = self.close()
      }

    final def map[R1](f: R => R1)
                     (implicit ec: ExecutionContext): Publication[T, R1] =
      new Publication[T, R1] {
        def apply(v1: T): Future[R1] = self(v1).map(f)
        def close(): Unit = self.close()
      }
  }

  def retry[T, R](
                   publication: Publication[T, R],
                   successCheck: R => Boolean,
                   initialDelay: Duration,
                   increment: Duration => Duration,
                   maxRetries: Long
                 )(implicit ec: ExecutionContext): Publication[T, R] =
    new Publication[T, R] {

      def apply(v1: T): Future[R] = {

        val p: Promise[R] = Promise[R]()

        // IDK how well this will stand up to the JVM's call stack limit
        def helper(retries: Long, delay: Duration): Unit = {

          publication(v1).onComplete({
            case Success(r) if successCheck(r) =>
              p.success(r)
            case Success(r) if !successCheck(r) && retries == 0 =>
              p.success(r)
            case Failure(e) if retries == 0 =>
              p.failure(e)
            case _ =>
              helper(retries - 1, increment(delay))
          })
        }

        Future { helper(maxRetries, initialDelay) }

        p.future
      }

      def close(): Unit = publication.close()
    }
}
