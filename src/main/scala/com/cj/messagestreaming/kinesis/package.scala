package com.cj.messagestreaming

import java.nio.ByteBuffer

import com.amazonaws.auth._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker._
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import org.slf4s.Logging

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

package object kinesis extends Logging {

  def makePublication[T](
                          config: KinesisProducerConfig,
                          serialize: T => Array[Byte]
                        ): Publication[T, PublishResult] = {

    val (send, close) = produce(
      streamName = config.streamName,
      producer = getKinesisProducer(
        accessKeyId = config.accessKeyId,
        secretKey = config.secretKey,
        region = config.region
      )
    )

    def sendAndLogOnError(t: T): Future[PublishResult] =
      send(serialize(t)).recoverWith { case err: Throwable =>
        log.error(s"Failed to send: $t. Cause: $err", err)
        Future.failed(err)
      }

    Publication(sendAndLogOnError, close(()))
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
      if (config.credentialsProvider.isDefined) {
        config.credentialsProvider.get
      }
      else {
        for {
          a <- config.accessKeyId
          s <- config.secretKey
        } yield new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
      }.getOrElse(new DefaultAWSCredentialsProviderChain)
    }
    
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

    // TODO:
    // If the worker shuts down, the associated subscription blocks forever
    //   - Should we notify the client if this happens?
    //   - Should we try to resurrect the worker if this happens?
    Future(Try(worker.run())).onComplete {
      case Success(_) =>
        log.error("Disaster strikes! Unexpected worker completion!")
      case Failure(e) =>
        log.error("Disaster strikes! Unexpected worker termination!", e)
    }

    sub
  }

  protected[kinesis] def produce(streamName: String, producer: KinesisProducer):
  (Array[Byte] => Future[PublishResult], Unit => Unit) = {

    var shutdown = false

    def asScalaFuture[A](lf: ListenableFuture[A]): Future[A] = {
      val p = Promise[A]
      Futures.addCallback(lf,
        new FutureCallback[A] {
          def onSuccess(result: A): Unit = p.success(result)

          def onFailure(t: Throwable): Unit = p.failure(t)
        })
      p.future
    }

    def send(byteArray: Array[Byte]): Future[PublishResult] = {
      if (!shutdown) {
        val time = System.currentTimeMillis.toString
        val bytes = ByteBuffer.wrap(byteArray)
        // TODO:
        // addUserRecord can throw if:
        //   (1) time is zero chars or bigger than 256 chars
        //       (can't happen, since user doesn't get to specify),
        //   (2) if bytes is bigger than 1MiB
        //       (we should probably notify the user somehow), or
        //   (3) the child process is dead
        //       (we should probably try to resurrect the child).
        val kinesisFuture = producer.addUserRecord(streamName, time, bytes)
        asScalaFuture(kinesisFuture).map(PublishResult.fromKinesis)
      } else {
        Future.failed(new Throwable("Publication is shutting down."))
      }
    }

    def close(u: Unit): Unit = {
      shutdown = true
      producer.flushSync()
      producer.destroy()
    }

    (send, close)
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

    val q = new IterableBlockingQueue[Checkpointable[T]]

    val factory = new IRecordProcessorFactory {
      override def createProcessor(): IRecordProcessor =
        new CheckpointingRecordProcessor(queue = q, readRecord = read)
    }

    (factory, Subscription(q.iterator.asScala))
  }
}
