package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, Future => FutureJ}
import java.util.function.{Function => FunctionJ}
import java.util.stream.{Stream => StreamJ}

import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.cj.messagestreaming.Java._
import com.cj.messagestreaming._

import scala.collection.JavaConverters._

object Java {

  class KinesisSubscriptionJ[T](
                                 config: KinesisConsumerConfig,
                                 read: FunctionJ[Record, T]
                               ) extends SubscriptionJ[T] {

    private val sub: Subscription[T] =
      makeSubscription(config, r => read.apply(r))

    def mapWithCheckpointing(f: FunctionJ[T, Unit]): Unit =
      sub.mapWithCheckpointing(x => f.apply(x))

    def stream(): StreamJ[Checkpointable[T]] =
      java.util.stream.StreamSupport.stream(sub.stream.asJava.spliterator(), false)
  }

  class KinesisPublicationJ[T](
                                config: KinesisProducerConfig,
                                serializer: FunctionJ[T, Array[Byte]]
                              ) extends PublicationJ[T, UserRecordResult] {

    private var shutdown = false

    private val producer: KinesisProducer =
      getKinesisProducer(config.accessKeyId, config.secretKey, config.region)

    private def makeFailure: FutureJ[UserRecordResult] =
      new FutureJ[UserRecordResult] {
        def isCancelled: Boolean = true

        def get(): UserRecordResult =
          throw new Throwable("Publication is shutting down.")

        def get(timeout: Long, unit: TimeUnit): UserRecordResult =
          throw new Throwable("Publication is shutting down.")

        def cancel(mayInterruptIfRunning: Boolean): Boolean = false

        def isDone: Boolean = true
      }

    def publish(t: T): FutureJ[UserRecordResult] =
      if (!shutdown) {
        val bytes = serializer.apply(t)
        val time = System.currentTimeMillis.toString
        val realbytes = ByteBuffer.wrap(bytes)
        producer.addUserRecord(config.streamName, time, realbytes)
      } else {
        makeFailure
      }

    def close(): Unit = {
      shutdown = true
      producer.flushSync()
      producer.destroy()
    }
  }
}
