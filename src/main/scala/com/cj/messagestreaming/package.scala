package com.cj

import com.amazonaws.services.kinesis.producer.Attempt

import scala.concurrent.Future

package object messagestreaming {
  type Subscription = Stream[(Array[Byte], CheckpointCallback)]
  
  type CheckpointCallback = () => Unit
  
  trait Publication extends (Array[Byte] => Future[PublishResult]) with Closable

  trait PublishResult {
    def getAttempts: List[Attempt]

    def getSequenceNumber: String

    def getShardId: String

    def isSuccessful: Boolean
  }

  trait Closable {
    def close(): Unit
  }

}
