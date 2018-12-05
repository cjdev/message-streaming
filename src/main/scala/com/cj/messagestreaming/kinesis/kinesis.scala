package com.cj.messagestreaming
package kinesis

import com.amazonaws.auth.AWSCredentialsProvider
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

case class KinesisProducerConfig private(
                                          accessKeyId: Option[String],
                                          secretKey: Option[String],
                                          region: Option[String],
                                          streamName: String,
                                          credentialsProvider: Option[AWSCredentialsProvider]
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

  def apply(
             streamName: String,
             accessKeyId: String = null,
             secretKey: String = null,
             region: String = null,
             credentialsProvider: AWSCredentialsProvider = null
           ): KinesisProducerConfig = KinesisProducerConfig(
    Option(accessKeyId), Option(secretKey), Option(region), streamName, Option(credentialsProvider)
  )
}

case class KinesisConsumerConfig private(
                                          accessKeyId: Option[String],
                                          secretKey: Option[String],
                                          region: Option[String],
                                          streamName: String,
                                          applicationName: String,
                                          workerId: String,
                                          initialPositionInStream: InitialPositionInStream,
                                          credentialsProvider: Option[AWSCredentialsProvider]
                                        ) {
  def summary: String =
    s"""KinesisConsumerConfig: $this
       |    accessKeyId: $accessKeyId
       |    secretKey:   ****
       |    region:      $region
       |    streamName:  $streamName
       |    workerId:    $workerId
       |    applicationName: $applicationName
       |    initialPositionInStream: $initialPositionInStream
       |""".stripMargin
}

object KinesisConsumerConfig {

  def apply(
             streamName: String,
             applicationName: String,
             workerId: String,
             accessKeyId: String = null,
             secretKey: String = null,
             region: String = null,
             initialPositionInStream: InitialPositionInStream = InitialPositionInStream.LATEST,
             credentialsProvider: AWSCredentialsProvider = null
           ): KinesisConsumerConfig = KinesisConsumerConfig(
    Option(accessKeyId), Option(secretKey), Option(region),
    streamName, applicationName, workerId, initialPositionInStream, Option(credentialsProvider)
  )
}
