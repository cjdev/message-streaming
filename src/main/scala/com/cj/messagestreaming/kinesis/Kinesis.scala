package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.cj.collections.{IterableBlockingQueue, IteratorStream}
import com.cj.messagestreaming.{Publication, Subscription}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


object Kinesis {
  case class KinesisProducerConfig private[Kinesis] (accessKeyId: Option[String], secretKey: Option[String], region: Option[String], streamName: String)
  object KinesisProducerConfig {
    def apply(streamName: String) {
      KinesisProducerConfig(None, None, None, streamName)
    }
    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String) {
      KinesisProducerConfig(Some(accessKeyId), Some(secretKey), Some(region), streamName)
    }
  }

  case class KinesisConsumerConfig private[Kinesis] (accessKeyId: Option[String], secretKey: Option[String], region: Option[String], streamName: String, applicationName: String, workerId: String)
  object KinesisConsumerConfig {
    def apply(streamName: String, applicationName: String, workerId: String) {
      KinesisConsumerConfig(None, None, None, streamName, applicationName, workerId)
    }

    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String, applicationName: String, workerId: String) {
      KinesisConsumerConfig(Some(accessKeyId), Some(secretKey), Some(region), streamName, applicationName, workerId)
    }
  }

  def makePublication(config: KinesisProducerConfig): Publication = {
    produce(config.streamName, getKinesisProducer(config.accessKeyId, config.secretKey, config.region))
  }

  def makeSubscription(config: KinesisConsumerConfig): Subscription = {
    val provider: AWSCredentialsProvider= {for {
      a <- config.accessKeyId
      s <- config.secretKey
    } yield new StaticCredentialsProvider(new BasicAWSCredentials(a, s))}.getOrElse(new DefaultAWSCredentialsProviderChain)
    val kinesisConfig = new KinesisClientLibConfiguration(config.applicationName, config.streamName, provider, config.workerId)
    config.region.foreach(kinesisConfig.withRegionName)

    val (recordProcessorFactory, stream) = subscribe()
    val worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kinesisConfig).build()

    Future {
      worker.run()
    }

    stream
  }

  protected[kinesis] def produce(streamName: String, producer: KinesisProducer): Publication = {
    (byteArray : Array[Byte])=> {
      producer.addUserRecord(streamName, System.currentTimeMillis.toString, ByteBuffer.wrap(byteArray))
    }
  }

  protected[kinesis] def getKinesisProducer(accessKeyId: Option[String], secretKey : Option[String], region : Option[String]) : KinesisProducer = {
    val provider = {for {
      a <- accessKeyId
      s <- secretKey
    } yield new StaticCredentialsProvider(new BasicAWSCredentials(a, s))}.getOrElse(new DefaultAWSCredentialsProviderChain)
    val cfg : KinesisProducerConfiguration = new KinesisProducerConfiguration()
    cfg.setCredentialsProvider(provider)
    region.foreach(cfg.setRegion)
    new KinesisProducer(cfg)
  }

  protected[kinesis] def subscribe(): (IRecordProcessorFactory, Subscription) = {
    val q = new IterableBlockingQueue[Array[Byte]]()
    val stream = new IteratorStream(q.iterator())
    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor = new IRecordProcessor {
        override def shutdown(shutdownInput: ShutdownInput): Unit = {}
        override def initialize(initializationInput: InitializationInput): Unit = {}
        override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
          processRecordsInput.getRecords.forEach(((record: Record) => q.add(record.getData.array())).asJava)
        }
      }
    }
    (factory, stream)
  }
}
