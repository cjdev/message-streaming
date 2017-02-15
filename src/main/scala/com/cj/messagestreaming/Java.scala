package com.cj.messagestreaming
import com.cj.messagestreaming.kinesis.Kinesis.KinesisProducerConfig

import scala.concurrent.Future

object Java {
  case class PublicationJ(publication: Publication) extends Publication {
    override def apply(bytes: Array[Byte]): Future[PublishResult] = publication(bytes)
    override def close(): Unit = publication.close()
  }

  case class SubscriptionJ(subscription:Subscription) extends Subscription {
    override def mapWithCheckpointing(f: (Array[Byte]) => Unit): Unit = subscription.mapWithCheckpointing(f)
    override def stream: Stream[CheckpointableRecord] = subscription.stream
  }

  object KinesisProducerConfigJ {
    def apply(accessKeyId: String, secretKey: String, region: String, streamName: String): KinesisProducerConfig = {
      KinesisProducerConfig(Some(accessKeyId), Some(secretKey), Some(region), streamName)
    }
  }
}
