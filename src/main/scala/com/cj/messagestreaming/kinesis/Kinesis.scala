package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{Attempt, KinesisProducer, KinesisProducerConfiguration, UserRecordResult}
import com.cj.collections.{IterableBlockingQueue, IteratorStream}
import com.cj.messagestreaming._
import org.slf4j.LoggerFactory

import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
import com.cj.util._

import scala.collection.mutable

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

  case class KinesisConsumerConfig private[Kinesis](accessKeyId: Option[String], secretKey: Option[String], region: Option[String], streamName: String, applicationName: String, workerId: String)

  object KinesisConsumerConfig {
    def apply(streamName: String, applicationName: String, workerId: String): KinesisConsumerConfig = {
      KinesisConsumerConfig(None, None, None, streamName, applicationName, workerId)
    }

    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String, applicationName: String, workerId: String): KinesisConsumerConfig = {
      KinesisConsumerConfig(Some(accessKeyId), Some(secretKey), Some(region), streamName, applicationName, workerId)
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
    val kinesisConfig = new KinesisClientLibConfiguration(config.applicationName, config.streamName, provider, config.workerId)
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

  protected[kinesis] def produce(streamName: String, producer: KinesisProducer): Publication = {
    new Publication {
      var shutdown = false

      def apply(byteArray: Array[Byte]) : Future[PublishResult] = {
        if (!shutdown) {
          val time = System.currentTimeMillis.toString
          val bytes = ByteBuffer.wrap(byteArray)
          val future = producer.addUserRecord(streamName, time, bytes)
          toScalaFuture(future) map (new KinesisPublishResult(_))
        } else {
          Future.failed[PublishResult](new Throwable("Publication is shutting down."))
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

  protected[kinesis] def subscribe(): (IRecordProcessorFactory, Subscription) = {
    var mostRecentRecordProcessed:Record = null
    var secondMostRecentRecordProcessed:Record = null
    def onProcess(record: Record): Unit ={
      secondMostRecentRecordProcessed = mostRecentRecordProcessed
      mostRecentRecordProcessed = record
    }

    val q = new IterableBlockingQueue[(Array[Byte], CheckpointCallback)]

    val stream = new IteratorStream(q.iterator())
    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor = new CheckpointingRecordProcessor(q)
    }
    (factory, stream)
  }

  class KinesisPublishResult(urr: UserRecordResult) extends PublishResult {
    override def getAttempts: List[Attempt] = urr.getAttempts.toList

    override def getSequenceNumber: String = urr.getSequenceNumber

    override def getShardId: String = urr.getShardId

    override def isSuccessful: Boolean = urr.isSuccessful
  }

}
