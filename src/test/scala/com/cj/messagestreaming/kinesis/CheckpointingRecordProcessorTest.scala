package com.cj.messagestreaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput, ShutdownReason}
import com.amazonaws.services.kinesis.model.Record
import com.cj.collections.IterableBlockingQueue
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import java.util

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CheckpointingRecordProcessorTest extends FlatSpec with Matchers {

  var sequence: Int = 0
  def makeRecord(s: String): Record = {
    new Record()
      .withData(ByteBuffer.wrap(s.getBytes))
      .withSequenceNumber({sequence += 1; sequence.toString})
  }

  def consumeOne[T](i: util.Iterator[T]): Boolean = {
    i.next // pulls out one thing
    i.hasNext // marks the thing as consumed
  }

  class Setup {
    var time = 0L
    val q = new IterableBlockingQueue[Array[Byte]]
    val i = q.iterator()
    val recordProcessor: IRecordProcessor = new CheckpointingRecordProcessor(q, time)
    recordProcessor.initialize(new InitializationInput)
    val checkpointer = new StubCheckpointer {
      var checkpoints: List[Record] = List()
      override def checkpoint(record: Record): Unit = checkpoints = record :: checkpoints
    }
    var records = List("x1", "x2", "x3").map(makeRecord _)
    val processRecordsInput: ProcessRecordsInput = new ProcessRecordsInput().withCheckpointer(checkpointer).withRecords(records)
  }

  "it" should "not checkpoint before a minute has elapsed" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    time = 50000
    consumeOne(i)

    checkpointer.checkpoints.size should be(0)
  }

  "it" should "checkpoint if a minute has elapsed" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    time = 500000
    consumeOne(i)

    checkpointer.checkpoints.head.getSequenceNumber should be(records.head.getSequenceNumber)
  }

  "it" should "wait for outstanding records to be acknowledged and checkpoint on shutdown" in new Setup {
    recordProcessor.processRecords(processRecordsInput)
    time=500000
    consumeOne(i)
    val done = new ShutdownInput().withShutdownReason(ShutdownReason.TERMINATE)

    val stop = Future { recordProcessor.shutdown(done) }

    i.foreach(_=>()) // consume remaining records

    Await.ready(stop, Duration.Inf)

    checkpointer.checkpoints.head.getSequenceNumber should be(records.last.getSequenceNumber)

  }

  "it" should "put records in the provided queue" in new Setup{
    recordProcessor.processRecords(processRecordsInput)
    val things = records.map(_.getData).map(thing => new String(thing.array(), "UTF-8"))

    val consumedRecords = q.iterator().map(new String(_, "UTF-8"))

    things.foreach ( r => consumedRecords.next should be (r) )
  }

}
