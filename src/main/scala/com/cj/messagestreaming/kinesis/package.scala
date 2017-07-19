package com.cj.messagestreaming

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

package object kinesis {

  private lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  def makePublication[T](
                          config: KinesisProducerConfig,
                          serialize: T => Array[Byte]
                        ): Publication[T, PublishResult] =
    new Publication[T, PublishResult] {

      private val producer = produce(
        config.streamName,
        getKinesisProducer(
          config.accessKeyId,
          config.secretKey,
          config.region
        )
      )

      def close(): Unit = producer.close()

      def apply(t: T): Future[PublishResult] = producer(serialize(t))
    }

  def makeRecordSubscription(config: KinesisConsumerConfig): Subscription[Record] =
    makeSubscription(config, identity)

  def makeSubscription(config: KinesisConsumerConfig): Subscription[Array[Byte]] =
    makeSubscription(config, record => record.getData.array)

  def makeSubscription[T](
                           config: KinesisConsumerConfig,
                           read: Record => T
                         ): Subscription[T] = {

    val provider: AWSCredentialsProvider = {
      for {
        a <- config.accessKeyId
        s <- config.secretKey
      } yield new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
    }.getOrElse(new DefaultAWSCredentialsProviderChain)

    val kinesisConfig = new KinesisClientLibConfiguration(
      config.applicationName,
      config.streamName,
      provider,
      config.workerId
    ).withInitialPositionInStream(config.initialPositionInStream)

    config.region.foreach(kinesisConfig.withRegionName)

    val (recordProcessorFactory, sub) = subscribe(read)

    val worker = new Worker.Builder()
      .recordProcessorFactory(recordProcessorFactory)
      .config(kinesisConfig).build()

    // TODO: If the worker shuts down, the associated subscription blocks forever
    // TODO: Should we notify the client if this happens?
    // TODO: Should we try to resurrect the worker if this happens?
    Future(Try(worker.run())).onComplete {
      case Success(_) =>
        logger.error("Disaster strikes! Unexpected worker completion!")
      case Failure(e) =>
        logger.error("Disaster strikes! Unexpected worker termination!", e)
    }

    sub
  }

  protected[kinesis] def produce(
                                  streamName: String,
                                  producer: KinesisProducer
                                ): Publication[Array[Byte], PublishResult] = {
    new Publication[Array[Byte], PublishResult] {

      private var shutdown = false

      def asScalaFuture[A](lf: ListenableFuture[A]): Future[A] = {
        val p = Promise[A]
        Futures.addCallback(lf,
          new FutureCallback[A] {
            def onSuccess(result: A): Unit = p.success(result)

            def onFailure(t: Throwable): Unit = p.failure(t)
          })
        p.future
      }

      def apply(byteArray: Array[Byte]): Future[PublishResult] = {
        if (!shutdown) {
          val time = System.currentTimeMillis.toString
          val bytes = ByteBuffer.wrap(byteArray)
          val kinesisFuture = producer.addUserRecord(streamName, time, bytes)
          asScalaFuture(kinesisFuture).map(PublishResult.fromKinesis)
        } else {
          Future.failed(new Throwable("Publication is shutting down."))
        }
      }

      def close(): Unit = {
        shutdown = true
        producer.flushSync()
        producer.destroy()
      }
    }
  }

  protected[kinesis] def getKinesisProducer(
                                             accessKeyId: Option[String],
                                             secretKey: Option[String],
                                             region: Option[String]
                                           ): KinesisProducer = {

    val provider = {
      for {
        a <- accessKeyId
        s <- secretKey
      } yield new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
    }.getOrElse(new DefaultAWSCredentialsProviderChain)

    val cfg: KinesisProducerConfiguration = new KinesisProducerConfiguration()

    cfg.setCredentialsProvider(provider)
    region.foreach(cfg.setRegion)
    cfg.setRateLimit(80)

    new KinesisProducer(cfg)
  }

  protected[kinesis] def subscribe[T](read: Record => T):
  (IRecordProcessorFactory, Subscription[T]) = {

    var mostRecentRecordProcessed: Record = null
    var secondMostRecentRecordProcessed: Record = null

    def onProcess(record: Record): Unit = {
      secondMostRecentRecordProcessed = mostRecentRecordProcessed
      mostRecentRecordProcessed = record
    }

    val q = new IterableBlockingQueue[Checkpointable[T]]

    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor =
        new CheckpointingRecordProcessor(queue = q, readRecord = read)
    }

    (factory, Subscription(q.stream))
  }
}
