//package com.cj.messagestreaming.pubsub
//
//import java.util.concurrent.{CompletableFuture, CompletionStage}
//
//import com.cj.messagestreaming.{Publication, Subscription}
//import com.cj.collections.{IterableBlockingQueue, IteratorStream}
//import com.spotify.google.cloud.pubsub.client.Puller.MessageHandler
//import com.spotify.google.cloud.pubsub.client.{Message, Publisher, Pubsub, Puller}
//
//object PubSub {
//  case class PubSubConfig(project: String, topic: String)
//
//  def makeSubscription(config: PubSubConfig, subscriberId: String): Subscription = {
//    val pubsubClient = Pubsub.builder().build()
//
//    pubsubClient.createSubscription(config.project, config.topic)
//
//    val (enqueueMessage, stream) = subscribe()
//
//    val handler : Puller.MessageHandler = new MessageHandler {
//      override def handleMessage(puller: Puller, s: String, message: Message, ackId: String): CompletionStage[String] =
//        enqueueMessage(message,ackId)
//    }
//
//    Puller.builder()
//      .pubsub(pubsubClient)
//      .project(config.project)
//      .subscription(subscriberId)
//      .messageHandler(handler)
//      .build()
//
//    stream
//  }
//
//  type AckId = String
//  type PubSubCallback = (Message, AckId) => CompletionStage[String]
//
//  protected[pubsub] def subscribe() : (PubSubCallback, Subscription) = {
//    //the pubsubcallback writes to a queue
//    //and the subscription reads from the same queue
//    val q = new IterableBlockingQueue[Array[Byte]]()
//    val stream = new IteratorStream(q.iterator)
//
//    def callback = (message : Message, ackId : AckId) => {
//      q.add(message.decodedData())
//      CompletableFuture.completedFuture(ackId)
//    }
//
//    (callback, stream)
//  }
//
//  def makePublication(config: PubSubConfig): Publication = {
//    val pubsubClient = Pubsub.builder().build()
//
//    pubsubClient.createTopic(config.project, config.topic)
//
//    val publisher = Publisher.builder()
//      .pubsub(pubsubClient)
//      .project(config.project)
//      .build()
//
//    publish(config.topic, publisher)
//  }
//
//  protected[pubsub] def publish(topic: String, publisher: Publisher) : Publication = {
//    (data: Array[Byte]) => publisher.publish(topic, Message.of(Message.encode(data)))
//  }
//}
