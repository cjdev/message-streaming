package com.cj.messagestreaming.kafka

import com.cj.messagestreaming.{Publication, Subscription}
import kafka.consumer.{ConsumerConfig, ConsumerConnector, ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.server.KafkaConfig

import scala.collection.Map

object Kafka {

  type KafkaStreamOfBytes = KafkaStream[Array[Byte], Array[Byte]]

  def makeSubscription(config: ConsumerConfig, topic: String): Subscription = {
    val numThreads: Integer = 1
    val consumer: ConsumerConnector = kafka.consumer.Consumer.create(config)

    val consumerMap: Map[String, List[KafkaStreamOfBytes]] = consumer.createMessageStreams(Map((topic, numThreads)))

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        consumer.shutdown()
      }
    })

//    val streams: List[KafkaStreamOfBytes] = consumerMap.getOrElse(topic, List())
//
//    streams match {
//      case head :: _ => head.iterator.toStream.map(_.message)
//      case _ => Stream()
//    }

    def getMessage(ksb:List[KafkaStreamOfBytes]) = ksb.headOption.map(_.map(_.message))
    consumerMap.get(topic).flatMap(getMessage).getOrElse(Stream()).toStream

    (for {
      x <- consumerMap.get(topic)
      y <- x.headOption
    } yield y.map(_.message).toStream).getOrElse(Stream())

  }

  def makePublication(config: KafkaConfig): Publication = {
    ???
  }
}
