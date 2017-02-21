package com.cj

import com.amazonaws.services.kinesis.producer.Attempt
import com.cj.collections.Streamable
import com.google.common.util.concurrent.ListenableFuture

package object messagestreaming {

  trait Subscription  extends Streamable[CheckpointableRecord] {
    def mapWithCheckpointing(f: Array[Byte] => Unit): Unit
  }

  object Subscription {
    def apply(stream: Stream[CheckpointableRecord]) = StreamSubscription(stream)
  }

  case class StreamSubscription(stream: Stream[CheckpointableRecord]) extends Subscription {
    override def mapWithCheckpointing(f: Array[Byte] => Unit): Unit = {
      stream.foreach{
        case CheckpointableRecord(data, callback) => {
          f(data)
          callback()
        }
      }
    }
  }
  
  case class CheckpointableRecord(data: Array[Byte], checkpointCallback: CheckpointCallback)
  
  type CheckpointCallback = () => Unit
  
  trait Publication extends (Array[Byte] => ListenableFuture[PublishResult]) with Closable

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
