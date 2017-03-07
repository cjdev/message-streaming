package com.cj.messagestreaming

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

package object kinesis {

  private lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  case class KinesisProducerConfig private[kinesis](
                                                     accessKeyId: Option[String],
                                                     secretKey: Option[String],
                                                     region: Option[String],
                                                     streamName: String
                                                   )

  object KinesisProducerConfig {
    def apply(streamName: String): KinesisProducerConfig = {
      KinesisProducerConfig(None, None, None, streamName)
    }

    def apply(
               accessKeyId: String,
               secretKey: String,
               region: String,
               streamName: String
             ): KinesisProducerConfig = {
      KinesisProducerConfig(
        Some(accessKeyId),
        Some(secretKey),
        Some(region),
        streamName
      )
    }
  }

  case class KinesisConsumerConfig private[kinesis](
                                                     accessKeyId: Option[String],
                                                     secretKey: Option[String],
                                                     region: Option[String],
                                                     streamName: String,
                                                     applicationName: String,
                                                     workerId: String,
                                                     initialPositionInStream: InitialPositionInStream
                                                   )

  object KinesisConsumerConfig {
    def apply(
               streamName: String,
               applicationName: String,
               workerId: String
             ): KinesisConsumerConfig = {
      KinesisConsumerConfig(
        accessKeyId = None,
        secretKey = None,
        region = None,
        streamName = streamName,
        applicationName = applicationName,
        workerId = workerId,
        initialPositionInStream = InitialPositionInStream.LATEST
      )
    }

    def apply(
               streamName: String,
               applicationName: String,
               workerId: String,
               initialPositionInStream: InitialPositionInStream
             ): KinesisConsumerConfig = {
      KinesisConsumerConfig(
        accessKeyId = None,
        secretKey = None,
        region = None,
        streamName = streamName,
        applicationName = applicationName,
        workerId = workerId,
        initialPositionInStream = initialPositionInStream
      )
    }

    def apply(
               accessKeyId: String,
               secretKey: String,
               region: String,
               streamName: String,
               applicationName: String,
               workerId: String
             ): KinesisConsumerConfig = {
      KinesisConsumerConfig(
        accessKeyId = Some(accessKeyId),
        secretKey = Some(secretKey),
        region = Some(region),
        streamName = streamName,
        applicationName = applicationName,
        workerId = workerId,
        initialPositionInStream = InitialPositionInStream.LATEST
      )
    }

    def apply(
               accessKeyId: String,
               secretKey: String,
               region: String,
               streamName: String,
               applicationName: String,
               workerId: String,
               initialPositionInStream: InitialPositionInStream
             ): KinesisConsumerConfig = {
      KinesisConsumerConfig(
        accessKeyId = Some(accessKeyId),
        secretKey = Some(secretKey),
        region = Some(region),
        streamName = streamName,
        applicationName = applicationName,
        workerId = workerId,
        initialPositionInStream = initialPositionInStream
      )
    }
  }

  def makePublication(config: KinesisProducerConfig): Publication[UserRecordResult] = {
    produce(
      config.streamName,
      getKinesisProducer(
        config.accessKeyId,
        config.secretKey,
        config.region
      )
    )
  }

  def makeSubscription(config: KinesisConsumerConfig): Subscription = {
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

    val (recordProcessorFactory, sub) = subscribe()
    val worker = new Worker.Builder()
      .recordProcessorFactory(recordProcessorFactory)
      .config(kinesisConfig).build()

    Future {
      Try(worker.run())
    }.onComplete({
      case Success(s) => logger.error(s"Disaster strikes! Unexpected worker completion. Message: $s")
      case Failure(e) => logger.error(s"Disaster strikes! The worker has terminated abnormally. Error: $e")
    })

    sub
  }

  protected[kinesis] def produce(
                                  streamName: String,
                                  producer: KinesisProducer
                                ): Publication[UserRecordResult] = {
    new Publication[UserRecordResult] {

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

      def apply(byteArray: Array[Byte]): Future[UserRecordResult] = {
        if (!shutdown) {
          val time = System.currentTimeMillis.toString
          val bytes = ByteBuffer.wrap(byteArray)
          val kinesisFuture = producer.addUserRecord(streamName, time, bytes)
          asScalaFuture(kinesisFuture)
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
    new KinesisProducer(cfg)
  }

  protected[kinesis] def subscribe(): (IRecordProcessorFactory, Subscription) = {
    var mostRecentRecordProcessed: Record = null
    var secondMostRecentRecordProcessed: Record = null

    def onProcess(record: Record): Unit = {
      secondMostRecentRecordProcessed = mostRecentRecordProcessed
      mostRecentRecordProcessed = record
    }

    val q = new IterableBlockingQueue[CheckpointableRecord]

    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor =
        new CheckpointingRecordProcessor(q)
    }

    (factory, Subscription(q.stream))
  }
}
