package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, Future => FutureJ}
import java.util.function.{Function => FunctionJ}
import java.util.stream.{Stream => StreamJ}

import com.amazonaws.services.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.cj.messagestreaming.Java._
import com.cj.messagestreaming._

import scala.collection.JavaConverters._

object Java {

  class KinesisSubscriptionJ(config: KinesisConsumerConfig)
    extends SubscriptionJ {

    private val sub: Subscription = makeSubscription(config)

    def mapWithCheckpointing(f: FunctionJ[Array[Byte], Unit]): Unit =
      sub.mapWithCheckpointing(x => f.apply(x))

    def stream(): StreamJ[CheckpointableRecord] =
      java.util.stream.StreamSupport.stream(sub.stream.toIterable.asJava.spliterator(), false)
  }

  class KinesisPublicationJ(config: KinesisProducerConfig)
    extends PublicationJ[UserRecordResult] {

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

    def publish(bytes: Array[Byte]): FutureJ[UserRecordResult] =
      if (!shutdown) {
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
