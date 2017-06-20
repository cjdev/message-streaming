package com.cj.messagestreaming.kinesis

import java.util.function.Consumer

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.cj.messagestreaming._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

class CheckpointingRecordProcessor(q: Queue[Checkpointable[Array[Byte]]], time: => Long = System.currentTimeMillis()) extends IRecordProcessor {

  private lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  var checkpointers: mutable.Queue[Future[Unit => Unit]] = mutable.Queue.empty
  val checkpointInterval = 60000L // one minute
  var nextCheckpointTime: Long = 0

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    logger.info(s"Shutting down record processor. Reason: ${shutdownInput.getShutdownReason}.")
    q.done()
    if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
      checkpointers.foreach(x => Await.ready(x, Duration.Inf))
      checkpoint()
    }
  }

  override def initialize(initializationInput: InitializationInput): Unit = {
    logger.info(s"Initializing record processor with shardId ${initializationInput.getShardId} and sequence number ${initializationInput.getExtendedSequenceNumber}.")
    nextCheckpointTime = time + checkpointInterval
  }

  def processOneRecord(record: Record, iRecordProccessorCheckpointer: IRecordProcessorCheckpointer): Unit = {
    val checkpointerPromise = Promise[Unit => Unit]
    checkpointers.enqueue(checkpointerPromise.future)
    val checkpointer: Unit => Unit = _ => {
      logger.info(s"Setting checkpoint: ${record.getSequenceNumber}")
      iRecordProccessorCheckpointer.checkpoint(record)
    }
    val markProcessedRecord: Unit => Unit = (_: Unit) => {
      checkpointerPromise.complete(Try(checkpointer))
      checkpointIfReady()
    }
    q.add(Checkpointable[Array[Byte]](record.getData.array(), (_: Unit) => markProcessedRecord()))

  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val process: Consumer[Record] = new Consumer[Record] {
      def accept(t: Record): Unit = processOneRecord(t, processRecordsInput.getCheckpointer)
    }
    processRecordsInput.getRecords.forEach(process)
  }

  def checkpointIfReady(): Unit = {
    if (time > nextCheckpointTime) {
      checkpoint()
    }
  }

  def checkpoint(): Unit = {
    val (done, remaining) = checkpointers.span(_.isCompleted)
    if (done.nonEmpty) {
      Await.result(done.last, Duration.Zero)() //we're awaiting a future that is already complete
    }
    checkpointers = remaining
    nextCheckpointTime = time + checkpointInterval
  }
}
