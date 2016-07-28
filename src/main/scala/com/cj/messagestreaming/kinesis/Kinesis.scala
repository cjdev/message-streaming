package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.BasicAWSCredentials
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
  case class KinesisProducerConfig(accessKeyId : String, secretKey : String, streamName : String)
  case class KinesisConsumerConfig(accessKeyId : String, secretKey : String, streamName : String, applicationName: String, workerId: String)

  def makePublication(config: KinesisProducerConfig): Publication = {
    produce(config.streamName, getKinesisProducer(config.accessKeyId, config.secretKey, None))
  }

  def makePublication(config: KinesisProducerConfig, region: String): Publication = {
    produce(config.streamName, getKinesisProducer(config.accessKeyId, config.secretKey, Some(region)))
  }

  protected def makeSubscription(config: KinesisConsumerConfig, region : Option[String]): Subscription = {
    val provider = new StaticCredentialsProvider(new BasicAWSCredentials(config.accessKeyId, config.secretKey))
    val kinesisConfig = new KinesisClientLibConfiguration(config.applicationName, config.streamName, provider, config.workerId)
    region.foreach(kinesisConfig.withRegionName(_))

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

  protected[kinesis] def getKinesisProducer(accessKeyId : String, secretKey : String, region : Option[String]) : KinesisProducer = {
    val provider : StaticCredentialsProvider = new StaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey))
    val cfg : KinesisProducerConfiguration = new KinesisProducerConfiguration().setCredentialsProvider(provider)
    region.foreach(cfg.setRegion(_))
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
