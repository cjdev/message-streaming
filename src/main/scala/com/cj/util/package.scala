package com.cj

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

package object util {

  implicit def toScalaFuture[T](lFuture: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]
    Futures.addCallback(lFuture,
      new FutureCallback[T] {
        def onSuccess(result: T) = p.success(result)
        def onFailure(t: Throwable) = p.failure(t)
      })
    p.future
  }

}
