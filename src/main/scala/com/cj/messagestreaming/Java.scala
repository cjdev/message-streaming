package com.cj.messagestreaming

import java.util.concurrent.{Future => FutureJ}
import java.util.function.{Function => FunctionJ}
import java.util.stream.{Stream => StreamJ}

object Java {

  abstract class SubscriptionJ[T] {

    def mapWithCheckpointing(f: FunctionJ[T, Unit]): Unit

    def stream(): StreamJ[Checkpointable[T]]
  }

  abstract class PublicationJ[T, R] {

    def publish(t: T): FutureJ[R]

    def close(): Unit
  }
}
