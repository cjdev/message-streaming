package com.cj.messagestreaming


import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.producer.Attempt
import com.cj.messagestreaming.kinesis.Kinesis
import com.cj.messagestreaming.kinesis.Kinesis.{KinesisConsumerConfig, KinesisProducerConfig, KinesisPublishResult}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.cj.messagestreaming.kinesis.Kinesis.guavify

object Java {

  case class PublishResultJ(pr: PublishResult) extends PublishResult {
    override def getAttempts: List[Attempt] = pr.getAttempts
    override def getSequenceNumber: String = pr.getSequenceNumber
    override def getShardId: String = pr.getShardId
    override def isSuccessful: Boolean = pr.isSuccessful
  }

  case class PublicationJ(configJ: KinesisProducerConfigJ) {
    val publication : Publication = Kinesis.makePublication(configJ.config)
    def publish(bytes: Array[Byte]): ListenableFuture[PublishResultJ] = Futures.transform(publication(bytes), PublishResultJ)
  }

  case class SubscriptionJ(configJ: KinesisConsumerConfigJ)  {
    val subscription : Subscription = Kinesis.makeSubscription(configJ.config)
    def mapWithCheckpointing(f: (Array[Byte]) => Unit): Unit = subscription.mapWithCheckpointing(f)
    def stream: Stream[CheckpointableRecord] = subscription.stream
  }

  case class KinesisProducerConfigJ(accessKeyId: String, secretKey: String, region: String, streamName: String) {
    val config = KinesisProducerConfig(accessKeyId, secretKey, region, streamName)
  }

  case class KinesisConsumerConfigJ(accessKeyId: String, secretKey: String, region: String, streamName: String, applicationName: String, workerId: String, initialPositionInStream: InitialPositionInStream) {
    val config = KinesisConsumerConfig(accessKeyId, secretKey, region, streamName, applicationName, workerId, initialPositionInStream)
  }
}
