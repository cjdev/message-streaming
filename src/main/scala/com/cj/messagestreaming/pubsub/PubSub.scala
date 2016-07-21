package com.cj.messagestreaming.pubsub

import java.util.concurrent.{CompletableFuture, CompletionStage, ConcurrentLinkedQueue}

import com.cj.messagestreaming.{Publication, Subscription}
import com.spotify.google.cloud.pubsub.client.Puller.MessageHandler
import com.spotify.google.cloud.pubsub.client.{Message, Publisher, Pubsub, Puller}

import scala.collection.JavaConverters

object PubSub {
  case class PubSubConfig(project: String, topic: String)

  def makeSubscription(config: PubSubConfig, subscriberId: String): Subscription = {
    val pubsubClient = Pubsub.builder().build()
    pubsubClient.createSubscription(config.project, config.topic)

    val (callback, stream) = subscribe()

    val handler : Puller.MessageHandler = new MessageHandler {
      override def handleMessage(puller: Puller, s: String, message: Message, s1: String): CompletionStage[String] =
        callback(puller,s,message,s1)
    }

    Puller.builder()
      .pubsub(pubsubClient)
      .project(config.project)
      .subscription(subscriberId)
      .concurrency(32)
      .messageHandler(handler)
      .build()

    stream
  }

  protected[pubsub] def subscribe( ) : ((Puller, String, Message, String) => CompletionStage[String], Subscription) = {
    val queue = new ConcurrentLinkedQueue[Array[Byte]]()
    val stream = JavaConverters.asScalaIteratorConverter(queue.iterator()).asScala.toStream
    def callback = (_ : Puller, _ : String, message : Message, ackId : String) => {
      queue.add(message.decodedData())
      CompletableFuture.completedFuture(ackId)
    }
    (callback, stream)
  }

  def makePublication(config: PubSubConfig): Publication = {
    val pubsubClient = Pubsub.builder().build()

    pubsubClient.createTopic(config.project, config.topic)

    val publisher = Publisher.builder()
      .pubsub(pubsubClient)
      .project(config.project)
      .concurrency(128)
      .build()

    publish(config.topic, publisher)
  }

  protected[pubsub] def publish(topic: String, publisher: Publisher) : Publication = {
    (data: Array[Byte]) => publisher.publish(topic, Message.of(Message.encode(data)))
  }
}
