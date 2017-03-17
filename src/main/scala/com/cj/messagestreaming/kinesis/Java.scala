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
    extends SubscriptionJ[Array[Byte]] {

    private val sub: Subscription[Array[Byte]] = makeSubscription(config)

    def mapWithCheckpointing(f: FunctionJ[Array[Byte], Unit]): Unit =
      sub.mapWithCheckpointing(x => f.apply(x))

    def stream(): StreamJ[Checkpointable[Array[Byte]]] =
      java.util.stream.StreamSupport.stream(sub.stream.toIterable.asJava.spliterator(), false)
  }

  class KinesisPublicationJ[T](
                                config: KinesisProducerConfig,
                                serializer: FunctionJ[T, Array[Byte]]
                              ) extends PublicationJ[T, PublishResult] {

    private var shutdown = false

    private val producer: KinesisProducer =
      getKinesisProducer(config.accessKeyId, config.secretKey, config.region)

    private def makeFailure: FutureJ[PublishResult] =
      new FutureJ[PublishResult] {
        def isCancelled: Boolean = true

        def get(): PublishResult =
          throw new Throwable("Publication is shutting down.")

        def get(timeout: Long, unit: TimeUnit): PublishResult =
          throw new Throwable("Publication is shutting down.")

        def cancel(mayInterruptIfRunning: Boolean): Boolean = false

        def isDone: Boolean = true
      }

    def publish(t: T): FutureJ[PublishResult] =
      if (!shutdown) {
        val bytes = serializer.apply(t)
        val time = System.currentTimeMillis.toString
        val realbytes = ByteBuffer.wrap(bytes)
        com.google.common.util.concurrent.Futures.transform(
          producer.addUserRecord(config.streamName, time, realbytes),
          new com.google.common.base.Function[UserRecordResult, PublishResult] {
            def apply(t: UserRecordResult): PublishResult =
              PublishResult.fromKinesis(t)
          })
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
