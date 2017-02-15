package com.cj.messagestreaming
import com.cj.messagestreaming.kinesis.Kinesis.KinesisProducerConfig

import scala.concurrent.Future

object Java {
  case class PublicationJ(publication: Publication) extends Publication {
    override def close(): Unit = publication.close()
    def publish(bytes: Array[Byte]): Future[PublishResult] = publication(bytes)
  }

  case class SubscriptionJ(subscription:Subscription) extends Subscription {
    override def mapWithCheckpointing(f: (Array[Byte]) => Unit): Unit = subscription.mapWithCheckpointing(f)
    override def stream: Stream[CheckpointableRecord] = subscription.stream
  }

  object KinesisProducerConfigJ {
    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String): KinesisProducerConfig = {
      KinesisProducerConfig(accessKeyId, secretKey, region, streamName)
    }
  }
}
