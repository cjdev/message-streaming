package com.cj

import com.amazonaws.services.kinesis.producer.Attempt
import com.cj.collections.Streamable

import scala.concurrent.Future

package object messagestreaming {  
  case class Subscription(val stream: Stream[CheckpointableRecord]) extends Streamable[CheckpointableRecord] {
    def mapWithCheckpointing(f: Array[Byte] => Unit): Unit = {
      stream.foreach{
        case CheckpointableRecord(data, callback) => {
          f(data)
          callback()
        }
      }
    }
  }
  
  case class CheckpointableRecord(data: Array[Byte], checkpointCallback: CheckpointCallback);  
  
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
