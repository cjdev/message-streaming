package com.cj.messagestreaming.pubsub

import java.util.concurrent.{CompletableFuture, CompletionStage, ConcurrentLinkedQueue}

import com.cj.messagestreaming.{Publication, Subscription}
import com.spotify.google.cloud.pubsub.client.Puller.MessageHandler
import com.spotify.google.cloud.pubsub.client.{Message, Publisher, Pubsub, Puller}

object PubSub {
  case class PubSubConfig(project: String, topic: String, subscription: String)

  protected[pubsub] def subscribe(config: PubSubConfig, pubsubClient: Pubsub): Subscription = {
    pubsubClient.createSubscription(config.project, config.topic)

    val queue = new ConcurrentLinkedQueue[Array[Byte]]()

    val handler: Puller.MessageHandler = new MessageHandler {
      override def handleMessage(puller: Puller, sub: String, message: Message, ackId: String): CompletionStage[String] = {
        queue.add(message.decodedData())
        CompletableFuture.completedFuture(ackId)
      }
    }

    Puller.builder()
      .pubsub(pubsubClient)
      .project(config.project)
      .subscription(config.subscription)
      .concurrency(32)
      .messageHandler(handler)
      .build()

    queue.stream()
  }

  protected[pubsub] def publish(config: PubSubConfig, pubsubClient: Pubsub): Publication = {
    pubsubClient.createTopic(config.project, config.topic)

    val publisher = Publisher.builder()
      .pubsub(pubsubClient)
      .project(config.project)
      .concurrency(128)
      .build()

    (data: Array[Byte]) => publisher.publish(config.topic, Message.of(Message.encode(data)))
  }

  def subscribe(config: PubSubConfig): Subscription = {
    subscribe(config, Pubsub.builder().build())
  }

  def publish(config: PubSubConfig): Publication = {
    publish(config, Pubsub.builder().build())
  }
}
