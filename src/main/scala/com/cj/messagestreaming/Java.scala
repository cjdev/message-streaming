package com.cj.messagestreaming

import java.util.concurrent.{Future => FutureJ}
import java.util.function.{Function => FunctionJ}

object Java {

  abstract class SubscriptionJ[T] {

    def mapWithCheckpointing(f: FunctionJ[T, Unit]): Unit
  }

  abstract class PublicationJ[T, R] {

    def publish(t: T): FutureJ[R]

    def close(): Unit
  }

}
