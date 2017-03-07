package com.cj.messagestreaming

import java.util.concurrent.{Future => FutureJ}
import java.util.function.{Function => FunctionJ}
import java.util.stream.{Stream => StreamJ}

object Java {

  abstract class SubscriptionJ {

    def mapWithCheckpointing(f: FunctionJ[Array[Byte], Unit]): Unit

    def stream(): StreamJ[CheckpointableRecord]
  }

  abstract class PublicationJ[R] {

    def publish(bytes: Array[Byte]): FutureJ[R]

    def close(): Unit
  }
}
