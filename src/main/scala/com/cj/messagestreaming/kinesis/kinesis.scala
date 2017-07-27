package com.cj.messagestreaming
package kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.producer.{Attempt, UserRecordResult}
import scala.collection.JavaConversions._

case class PublishAttempt(
                           getDelay: Int,
                           getDuration: Int,
                           getErrorMessage: String,
                           getErrorCode: String,
                           isSuccessful: Boolean
                         )

object PublishAttempt {
  def fromKinesis(attempt: Attempt): PublishAttempt = PublishAttempt(
    getDelay = attempt.getDelay,
    getDuration = attempt.getDuration,
    getErrorMessage = attempt.getErrorMessage,
    getErrorCode = attempt.getErrorCode,
    isSuccessful = attempt.isSuccessful
  )
}

case class PublishResult(
                          getAttempts: List[PublishAttempt],
                          getSequenceNumber: String,
                          getShardId: String,
                          isSuccessful: Boolean
                        )

object PublishResult {
  def fromKinesis(urr: UserRecordResult): PublishResult = PublishResult(
    getAttempts = urr.getAttempts.map(PublishAttempt.fromKinesis).toList,
    getSequenceNumber = urr.getSequenceNumber,
    getShardId = urr.getShardId,
    isSuccessful = urr.isSuccessful
  )
}

case class KinesisProducerConfig private[kinesis](
                                                   accessKeyId: Option[String],
                                                   secretKey: Option[String],
                                                   region: Option[String],
                                                   streamName: String
                                                 ) {
  def summary: String =
    s"""KinesisProducerConfig: $this
       |    accessKeyId: $accessKeyId
       |    secretKey:   ****
       |    region:      $region
       |    streamName:  $streamName
       |""".stripMargin
}

object KinesisProducerConfig {
  def apply(streamName: String): KinesisProducerConfig = {
    KinesisProducerConfig(None, None, None, streamName)
  }

  def apply(
             accessKeyId: String,
             secretKey: String,
             region: String,
             streamName: String
           ): KinesisProducerConfig = {
    KinesisProducerConfig(
      Some(accessKeyId),
      Some(secretKey),
      Some(region),
      streamName
    )
  }
}

case class KinesisConsumerConfig private[kinesis](
                                                   accessKeyId: Option[String],
                                                   secretKey: Option[String],
                                                   region: Option[String],
                                                   streamName: String,
                                                   applicationName: String,
                                                   workerId: String,
                                                   initialPositionInStream: InitialPositionInStream
                                                 ) {
  def summary: String =
    s"""KinesisConsumerConfig: $this
       |    accessKeyId: $accessKeyId
       |    secretKey:   ****
       |    region:      $region
       |    streamName:  $streamName
       |    applicationName: $applicationName
       |    initialPositionInStream: $initialPositionInStream
       |""".stripMargin
}

object KinesisConsumerConfig {
  def apply(
             streamName: String,
             applicationName: String,
             workerId: String
           ): KinesisConsumerConfig = {
    KinesisConsumerConfig(
      accessKeyId = None,
      secretKey = None,
      region = None,
      streamName = streamName,
      applicationName = applicationName,
      workerId = workerId,
      initialPositionInStream = InitialPositionInStream.LATEST
    )
  }

  def apply(
             streamName: String,
             applicationName: String,
             workerId: String,
             initialPositionInStream: InitialPositionInStream
           ): KinesisConsumerConfig = {
    KinesisConsumerConfig(
      accessKeyId = None,
      secretKey = None,
      region = None,
      streamName = streamName,
      applicationName = applicationName,
      workerId = workerId,
      initialPositionInStream = initialPositionInStream
    )
  }

  def apply(
             accessKeyId: String,
             secretKey: String,
             region: String,
             streamName: String,
             applicationName: String,
             workerId: String
           ): KinesisConsumerConfig = {
    KinesisConsumerConfig(
      accessKeyId = Some(accessKeyId),
      secretKey = Some(secretKey),
      region = Some(region),
      streamName = streamName,
      applicationName = applicationName,
      workerId = workerId,
      initialPositionInStream = InitialPositionInStream.LATEST
    )
  }

  def apply(
             accessKeyId: String,
             secretKey: String,
             region: String,
             streamName: String,
             applicationName: String,
             workerId: String,
             initialPositionInStream: InitialPositionInStream
           ): KinesisConsumerConfig = {
    KinesisConsumerConfig(
      accessKeyId = Some(accessKeyId),
      secretKey = Some(secretKey),
      region = Some(region),
      streamName = streamName,
      applicationName = applicationName,
      workerId = workerId,
      initialPositionInStream = initialPositionInStream
    )
  }
}
