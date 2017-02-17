package com.cj.messagestreaming


import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.cj.messagestreaming.kinesis.Kinesis
import com.cj.messagestreaming.kinesis.Kinesis.{KinesisConsumerConfig, KinesisProducerConfig}
import com.google.common.util.concurrent.ListenableFuture


object Java {
  case class PublicationJ(configJ: KinesisProducerConfigJ) {
    val publication : Publication = Kinesis.makePublication(configJ.config)
    def publish(bytes: Array[Byte]): ListenableFuture[PublishResult] = publication(bytes)
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
