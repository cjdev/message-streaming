package com.cj

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

package object messagestreaming {

  trait Queue[T] {
    def add(t: T): Unit

    def done(): Unit
  }

  trait Closable {
    def close(): Unit
  }

  sealed trait Subscription[+T] extends Iterator[Checkpointable[T]] {
    def mapWithCheckpointing(f: T => Unit): Unit
  }

  object Subscription {

    def apply[T](it: Iterator[Checkpointable[T]]): Subscription[T] =
      new Subscription[T] {

        def mapWithCheckpointing(f: T => Unit): Unit =
          it.foreach { case Checkpointable(data, callback) => f(data); callback() }

        def hasNext: Boolean = it.hasNext

        def next(): Checkpointable[T] = it.next()
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

  sealed trait Publication[-T, +R] extends (T => Future[R]) with Closable {

    final def premap[T1](f: T1 => T): Publication[T1, R] =
      Publication(t1 => this.apply(f(t1)), this.close())

    final def map[R1](f: R => R1)
                     (implicit ec: ExecutionContext): Publication[T, R1] =
      Publication(t => this.apply(t).map(f), this.close())
  }

  object Publication {

    def apply[T, R](send: T => Future[R], onClose: => Unit): Publication[T, R] =
      new Publication[T, R] {

        private lazy val runClose: Unit = onClose

        def apply(v1: T): Future[R] =
          Try(send(v1)).recover { case e => Future.failed(e) }.get

        def close(): Unit = runClose
      }

    def blocking[T, R](responseTimeout: Duration)
                      (publication: Publication[T, R]): Publication[T, R] = {

      def block(t: T): Future[R] = Await.ready(publication(t), responseTimeout)

      Publication(block, publication.close())
    }

    def retrying[T, R](
                        maxRetries: Long,
                        responseTimeout: Duration = 5 seconds,
                        initialDelay: Duration = 100 millis,
                        incrementDelay: Duration => Duration = 2 * _,
                        maxDelay: Duration = 30 seconds
                      )(publication: Publication[T, R])
                      (implicit ec: ExecutionContext): Publication[T, R] = {

      def retry(v1: T, retries: Long, delay: Duration): Try[R] =
        Try(Await.result(publication(v1), responseTimeout)) match {
          case s@Success(_) => s
          case f@Failure(_) if retries <= 0 => f
          case _ =>
            Thread.sleep((delay min maxDelay).toMillis)
            retry(v1, retries - 1, incrementDelay(delay))
        }

      def begin(v1: T): Future[R] =
        Future(retry(v1, maxRetries, initialDelay).get)

      Publication(begin, publication.close())
    }
  }

}
