package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer
import java.util.concurrent.{Executor, TimeUnit}

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.{KinesisProducer, UserRecordResult}
import com.google.common.util.concurrent.ListenableFuture
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class KinesisTest extends FlatSpec with Matchers {

  val messagesToSend: List[Array[Byte]] =
    List(
      "message 1".getBytes(),
      "message 2".getBytes(),
      "message 3".getBytes()
    )

  "subscribe" should "put things in the stream when new records are processed" in {
    //given
    val (factory, sub) = subscribe(r => r.getData.array)
    val checkpointer = new StubCheckpointer()
    val recordsToSend: List[Record] =
      messagesToSend.map { bytes =>
        val record = new Record
        record.setData(ByteBuffer.wrap(bytes))
        record
      }

    //when
    factory.createProcessor().processRecords(
      new ProcessRecordsInput()
        .withRecords(recordsToSend.asJava)
        .withCheckpointer(checkpointer)
    )

    //then
    messagesToSend.zip(sub.take(3).toList).foreach({ case (x, y) => x should be(y.data) })
  }

  "produce" should "use the provided kinesis producer and stream name" in {
    //given
    val streamName = "myStream"
    var sentMessages: List[Array[Byte]] = Nil
    val stubProducer: KinesisProducer = new KinesisProducer {
      override def addUserRecord(
                                  stream: String,
                                  partitionKey: String,
                                  data: ByteBuffer
                                ): ListenableFuture[UserRecordResult] = {
        if (stream == streamName) sentMessages = sentMessages :+ data.array()
        new ListenableFuture[UserRecordResult] {
          def addListener(runnable: Runnable, executor: Executor): Unit = {}
          def cancel(b: Boolean): Boolean = false
          def isCancelled: Boolean = false
          def isDone: Boolean = true
          def get(): UserRecordResult = null
          def get(l: Long, timeUnit: TimeUnit): UserRecordResult = null
        }
      }
    }
    val publish = produce(streamName, stubProducer)

    //when
    messagesToSend.foreach(publish._1)

    //then
    sentMessages should be(messagesToSend)
  }
}
