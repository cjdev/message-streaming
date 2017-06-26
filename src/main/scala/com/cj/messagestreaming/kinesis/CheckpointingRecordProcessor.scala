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

class CheckpointingRecordProcessor[A](
                                       queue: Queue[Checkpointable[A]],
                                       time: => Long = System.currentTimeMillis(),
                                       readRecord: Record => A
                                     ) extends IRecordProcessor {

  private lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  private var checkpointers: mutable.Queue[Future[Unit => Unit]] = mutable.Queue.empty
  private val checkpointInterval = 60000L // one minute
  private var nextCheckpointTime: Long = 0

  private def processOneRecord(
                                record: Record,
                                iRecordProccessorCheckpointer: IRecordProcessorCheckpointer
                              ): Unit = {

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

    queue.add(Checkpointable(readRecord(record), (_: Unit) => markProcessedRecord()))
  }

  private def checkpointIfReady(): Unit = {
    if (time > nextCheckpointTime) {
      checkpoint()
    }
  }

  private def checkpoint(): Unit = {
    val (done, remaining) = checkpointers.span(_.isCompleted)
    if (done.nonEmpty) {
      Await.result(done.last, Duration.Zero)() //we're awaiting a future that is already complete
    }
    checkpointers = remaining
    nextCheckpointTime = time + checkpointInterval
  }

  override def initialize(initializationInput: InitializationInput): Unit = {
    logger.info(s"Initializing record processor with shardId ${initializationInput.getShardId} and sequence number ${initializationInput.getExtendedSequenceNumber}.")
    nextCheckpointTime = time + checkpointInterval
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    processRecordsInput.getRecords.forEach(new Consumer[Record] {
      def accept(t: Record): Unit = processOneRecord(t, processRecordsInput.getCheckpointer)
    })
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    logger.info(s"Shutting down record processor. Reason: ${shutdownInput.getShutdownReason}.")
    queue.done()
    if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
      checkpointers.foreach(x => Await.ready(x, Duration.Inf))
      checkpoint()
    }
  }
}
