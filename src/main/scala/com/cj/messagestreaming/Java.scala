package com.cj.messagestreaming

object Java {
  case class PublicationJ(publication: Publication) extends Publication {
    override def close(): Unit = publication.close()
  }

  case class SubscriptionJ(subscription:Subscription) extends Subscription(subscription.stream) {}
}
