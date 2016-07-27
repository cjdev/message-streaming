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

object Kinesis {
  case class KinesisProducerConfig(accessKeyId : String, secretKey : String, streamName : String)
  case class KinesisConsumerConfig(accessKeyId : String, secretKey : String, streamName : String, applicationName: String, workerId: String)

  def makePublication(config: KinesisProducerConfig): Publication = {
    produce(config.streamName, getKinesisProducer(config.accessKeyId, config.secretKey))
  }

  def makeSubscription(config: KinesisConsumerConfig): Subscription = {
    val provider = new StaticCredentialsProvider(new BasicAWSCredentials(config.accessKeyId, config.secretKey))
    val kinesisConfig = new KinesisClientLibConfiguration(config.applicationName, config.streamName, provider, config.workerId)

    val (recordProcessorFactory, stream) = subscribe()
    print("starting worker")
    new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kinesisConfig).build().run()
    stream
  }

  protected[kinesis] def produce(streamName: String, producer: KinesisProducer): Publication = {
    (byteArray : Array[Byte])=> {
      producer.addUserRecord(streamName, System.currentTimeMillis.toString, ByteBuffer.wrap(byteArray))
    }
  }

  protected[kinesis] def getKinesisProducer(accessKeyId : String, secretKey : String) : KinesisProducer = {
    val provider : StaticCredentialsProvider = new StaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey))
    val cfg : KinesisProducerConfiguration = new KinesisProducerConfiguration()
      .setCredentialsProvider(provider)
    new KinesisProducer(cfg)
  }

  protected[kinesis] def subscribe(): (IRecordProcessorFactory, Subscription) = {
    print("Setting up subscription")
    val q = new IterableBlockingQueue[Array[Byte]]()
    val stream = new IteratorStream(q.iterator())
    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor = new IRecordProcessor {
        override def shutdown(shutdownInput: ShutdownInput): Unit = {}
        override def initialize(initializationInput: InitializationInput): Unit = {}
        override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
          print("Got some records")
          processRecordsInput.getRecords.forEach(((record: Record) => q.add(record.getData.array())).asJava)
        }
      }
    }
    (factory, stream)
  }

  protected[kinesis] def doNothing() = {}
}
