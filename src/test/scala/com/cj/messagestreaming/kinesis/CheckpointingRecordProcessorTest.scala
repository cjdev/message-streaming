package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer
import java.util

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.model.Record
import com.cj.messagestreaming._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CheckpointingRecordProcessorTest extends FlatSpec with Matchers {

  var sequence: Int = 0

  def makeRecord(s: String): Record = {
    new Record()
      .withData(ByteBuffer.wrap(s.getBytes))
      .withSequenceNumber({ sequence += 1; sequence.toString })
  }

  def consumeOne(i: util.Iterator[Checkpointable[Array[Byte]]]): Unit = {
    i.next.checkpointCallback() // pulls out one thing and calls the callback (marks it as consumed)
  }

  class Setup {
    var time = 0L
    val q = new IterableBlockingQueue[Checkpointable[Array[Byte]]]
    val i = q.iterator()
    val recordProcessor = new CheckpointingRecordProcessor(
      queue = q, time = time, readRecord = record => record.getData.array()
    )
    recordProcessor.initialize(new InitializationInput)
    val checkpointer = new StubCheckpointer {
      var checkpoints: List[Record] = List()

      override def checkpoint(record: Record): Unit = checkpoints = record :: checkpoints
    }
    var records = List("x1", "x2", "x3").map(makeRecord _)
    val processRecordsInput: ProcessRecordsInput = new ProcessRecordsInput().withCheckpointer(checkpointer).withRecords(records)
  }

  behavior of "A CheckpointingRecordProcessor"

  it should "not checkpoint before a minute has elapsed" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    time = 50000
    consumeOne(i)

    checkpointer.checkpoints.size should be(0)
  }

  it should "checkpoint if a minute has elapsed" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    time = 500000
    consumeOne(i)

    checkpointer.checkpoints.head.getSequenceNumber should be(records.head.getSequenceNumber)
  }

  it should "wait for outstanding records to be acknowledged and checkpoint on shutdown" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    time = 500000
    consumeOne(i)
    val done = new ShutdownInput().withShutdownReason(ShutdownReason.TERMINATE)

    val stop = Future {
      recordProcessor.shutdown(done)
    }

    i.foreach(_.checkpointCallback()) // consume remaining records

    Await.ready(stop, Duration.Inf)

    checkpointer.checkpoints.head.getSequenceNumber should be(records.last.getSequenceNumber)

  }

  it should "put records in the provided queue" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    val things = records.map(_.getData).map(thing => new String(thing.array(), "UTF-8"))

    val consumedRecords = q.iterator().map(t => new String(t.data, "UTF-8"))

    things.foreach(r => consumedRecords.next should be(r))
  }
}

class StubCheckpointer extends IRecordProcessorCheckpointer {
  override def checkpoint(): Unit = {}

  override def checkpoint(record: Record): Unit = {}

  // checkpoints = record :: checkpoints
  override def checkpoint(sequenceNumber: String): Unit = {}

  override def checkpoint(sequenceNumber: String, subSequenceNumber: Long): Unit = {}
}
