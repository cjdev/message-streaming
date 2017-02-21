package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{Attempt, KinesisProducer, KinesisProducerConfiguration, UserRecordResult}
import com.cj.collections.{IterableBlockingMultiQueue, IterableBlockingQueue}
import com.cj.messagestreaming._
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Kinesis {

  lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  case class KinesisProducerConfig private[Kinesis](accessKeyId: Option[String], secretKey: Option[String], region: Option[String], streamName: String)

  object KinesisProducerConfig {
    def apply(streamName: String): KinesisProducerConfig = {
      KinesisProducerConfig(None, None, None, streamName)
    }

    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String): KinesisProducerConfig = {
      KinesisProducerConfig(Some(accessKeyId), Some(secretKey), Some(region), streamName)
    }
  }

  case class KinesisConsumerConfig private[Kinesis]( accessKeyId: Option[String],
                                                     secretKey: Option[String],
                                                     region: Option[String],
                                                     streamName: String,
                                                     applicationName: String,
                                                     workerId: String,
                                                     initialPositionInStream: InitialPositionInStream)

  object KinesisConsumerConfig {
    def apply(streamName: String, applicationName: String, workerId: String): KinesisConsumerConfig = {
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

    def apply(streamName: String, applicationName: String, workerId: String, initialPositionInStream: InitialPositionInStream): KinesisConsumerConfig = {
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

    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String, applicationName: String, workerId: String): KinesisConsumerConfig = {
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

    def apply( accessKeyId: String,
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

  def makePublication(config: KinesisProducerConfig): Publication = {
    produce(config.streamName, getKinesisProducer(config.accessKeyId, config.secretKey, config.region))
  }

  def makeSubscription(config: KinesisConsumerConfig): Subscription = {
    val provider: AWSCredentialsProvider = {
      for {
        a <- config.accessKeyId
        s <- config.secretKey
      } yield new StaticCredentialsProvider(new BasicAWSCredentials(a, s))
    }.getOrElse(new DefaultAWSCredentialsProviderChain)
    val kinesisConfig = new KinesisClientLibConfiguration(
      config.applicationName,
      config.streamName,
      provider,
      config.workerId
    ).withInitialPositionInStream(config.initialPositionInStream)
    config.region.foreach(kinesisConfig.withRegionName)

    val (recordProcessorFactory, stream) = subscribe()
    val worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kinesisConfig).build()

    Future {
      Try(worker.run())
    }.onComplete({
      case Success(s) => logger.error(s"Disaster strikes! Unexpected worker completion. Message: $s")
      case Failure(e) => logger.error(s"Disaster strikes! The worker has terminated abnormally. Error: $e")
    })

    stream
  }

  implicit def guavify[T,U](f: Function[T,U]) : com.google.common.base.Function[T, U] = {
    new com.google.common.base.Function[T, U]() {
      override def apply(t: T): U = f(t)
    }
  }

  protected[kinesis] def produce(streamName: String, producer: KinesisProducer): Publication = {
    new Publication {
      var shutdown = false

      def apply(byteArray: Array[Byte]) : ListenableFuture[PublishResult] = {
        if (!shutdown) {
          val time = System.currentTimeMillis.toString
          val bytes = ByteBuffer.wrap(byteArray)
          val kinesisFuture = producer.addUserRecord(streamName, time, bytes)

          val getPublishResultFromUserRecordResult: UserRecordResult => KinesisPublishResult  = new KinesisPublishResult(_)

          Futures.transform(kinesisFuture, getPublishResultFromUserRecordResult)

        } else {
          Futures.immediateFailedFuture(new Throwable("Publication is shutting down."))
        }
      }

      def close() = {
        shutdown = true
        producer.flushSync()
        producer.destroy()
      }
    }
  }

  def getKinesisProducer(accessKeyId: Option[String], secretKey: Option[String], region: Option[String]): KinesisProducer = {
    val provider = {
      for {
        a <- accessKeyId
        s <- secretKey
      } yield new StaticCredentialsProvider(new BasicAWSCredentials(a, s))
    }.getOrElse(new DefaultAWSCredentialsProviderChain)
    val cfg: KinesisProducerConfiguration = new KinesisProducerConfiguration()
    cfg.setCredentialsProvider(provider)
    region.foreach(cfg.setRegion)
    new KinesisProducer(cfg)
  }

  case class OrderedRecord(record: CheckpointableRecord, orderKey: (String,String))

  val recordPriority = new Ordering[OrderedRecord] {

    override def compare(x: OrderedRecord, y: OrderedRecord): Int = {
      if (x.orderKey._1 == x.orderKey._2) {
        x.orderKey._2.compare(y.orderKey._2)
      } else {
        (-1) * x.orderKey._1.compare(y.orderKey._1)
      }
    }
  }

  protected[kinesis] def subscribe(): (IRecordProcessorFactory, Subscription) = {
    var mostRecentRecordProcessed:Record = null
    var secondMostRecentRecordProcessed:Record = null
    def onProcess(record: Record): Unit ={
      secondMostRecentRecordProcessed = mostRecentRecordProcessed
      mostRecentRecordProcessed = record
    }

    val q: IterableBlockingMultiQueue[OrderedRecord] =
      IterableBlockingMultiQueue(priority = recordPriority, bound = 100)

    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor = {
        new CheckpointingRecordProcessor(q.newAdder())
      }
    }

    val s: Stream[CheckpointableRecord] = q.stream().map(_.record)

    (factory, Subscription(s))
  }

  class KinesisPublishResult(urr: UserRecordResult) extends PublishResult {
    override def getAttempts: List[Attempt] = urr.getAttempts.toList

    override def getSequenceNumber: String = urr.getSequenceNumber

    override def getShardId: String = urr.getShardId

    override def isSuccessful: Boolean = urr.isSuccessful
  }

}
