package com.cj.messagestreaming
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
}
