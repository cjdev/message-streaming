package com.cj.messagestreaming

import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.higherKinds
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class Checkpointable[+T](data: T, checkpointCallback: CheckpointCallback) {

  def map[U](f: T => U): Checkpointable[U] =
    Checkpointable(f(data), checkpointCallback)

  def flatMap[U](k: T => Checkpointable[U]): Checkpointable[U] =
    k(data) match {
      case Checkpointable(newData, newCallback) =>
        Checkpointable(newData, _ => { checkpointCallback(()); newCallback(()) })
    }

  def run: T = { checkpointCallback(()); data }
}

object Checkpointable {
  def point[T](data: T): Checkpointable[T] = Checkpointable(data, _ => {})
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

sealed trait Publication[-T, +R] extends (T => Future[R]) with Closable {

  final def bind(tf: Future[T])(implicit ec: ExecutionContext): Future[R] =
    tf.flatMap(this.apply)

  final def bindTry(tt: Try[T])(implicit ec: ExecutionContext): Future[R] =
    Future.fromTry(tt).flatMap(this.apply)

  final def traverse[M[X] <: TraversableOnce[X], T1 <: T, R1 >: R](ts: M[T1])
  (implicit cbf: CanBuildFrom[M[T1], R1, M[R1]], ec: ExecutionContext): Future[M[R1]] =
    Future.traverse(ts)(this.apply)

  final def block(timeout: Duration)(t: T): Try[R] =
    Try { Await.result(this.apply(t), timeout) }

  final def premap[T1](f: T1 => T): Publication[T1, R] =
    Publication(t1 => this.apply(f(t1)), this.close())

  final def map[R1](f: R => R1)(implicit ec: ExecutionContext): Publication[T, R1] =
    Publication(t => this.apply(t).map(f), this.close())
}

object Publication {

  /**
    * Create a [[Publication]] with the following guarantees:
    *   * `apply` will not throw (exceptions pushed to the returned [[Future]]),
    *   * `close` will have an effect only the first time is is used.
    */
  def apply[T, R](send: T => Future[R], onClose: => Unit): Publication[T, R] =
    new Publication[T, R] {

      private lazy val runClose: Unit = onClose

      def apply(v1: T): Future[R] =
        Try(send(v1)).recover { case e => Future.failed(e) }.get

      def close(): Unit = runClose
    }

  /**
    * Create a [[Publication]] that produces completed [[Future]]s.
    *
    * You should not reuse the original publication.
    */
  def blocking[T, R](responseTimeout: Duration)
                    (original: Publication[T, R]): Publication[T, R] = {

    def block(t: T): Future[R] = Await.ready(original(t), responseTimeout)

    Publication(block, original.close())
  }

  /**
    * Create a [[Publication]] that retries on failures, subject to the
    * parameters you supply.
    *
    * You should not reuse the original publication.
    */
  def retrying[T, R](
                      maxRetries: Long,
                      responseTimeout: Duration = 5 seconds,
                      initialDelay: Duration = 100 millis,
                      incrementDelay: Duration => Duration = 2 * _,
                      maxDelay: Duration = 30 seconds
                    )(original: Publication[T, R])
                     (implicit ec: ExecutionContext): Publication[T, R] = {

    def retry(v1: T, retries: Long, delay: Duration): Try[R] =
      Try(Await.result(original(v1), responseTimeout)) match {
        case s@Success(_) => s
        case f@Failure(_) if retries <= 0 => f
        case _ =>
          Thread.sleep((delay min maxDelay).toMillis)
          retry(v1, retries - 1, incrementDelay(delay))
      }

    def begin(v1: T): Future[R] =
      Future(retry(v1, maxRetries, initialDelay).get)

    Publication(begin, original.close())
  }

  /**
    * Creates a [[Publication]] that combines the effects of the supplied
    * functions. Useful for adding arbitrary effects, for example logging.
    *
    * You should not reuse the original publication.
    */
  def decorate[T, R, T1, R1](
                              preprocess: T1 => T,
                              postprocess: T1 => R => R1
                            )(original: Publication[T, R])
                             (implicit ec: ExecutionContext): Publication[T1, R1] = {

    def deco = (t1: T1) => original(preprocess(t1)).map(postprocess(t1))

    Publication[T1, R1](deco, original.close())
  }

  /**
    * Create a [[Publication]] that transfers the context provided by a
    * traversable functor `M` of a wrapped input value to the result value.
    *
    * You should not reuse the original publication.
    */
  def traversed[M[X] <: TraversableOnce[X], T, R](original: Publication[T, R])
  (implicit cbf: CanBuildFrom[M[T], R, M[R]], ec: ExecutionContext):
  Publication[M[T], M[R]] =
    Publication(Future.traverse(_)(original), original.close())
}
