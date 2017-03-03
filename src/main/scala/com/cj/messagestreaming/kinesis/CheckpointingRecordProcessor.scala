package com.cj.messagestreaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput, ShutdownReason}
import com.amazonaws.services.kinesis.model.Record
import com.cj.collections.{Queue, IterableBlockingQueue}
import org.slf4j.LoggerFactory
import com.cj.messagestreaming._
import scala.collection.mutable
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CheckpointingRecordProcessor(q: Queue[CheckpointableRecord], time: =>Long = System.currentTimeMillis()) extends IRecordProcessor {
    lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)
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
      val markProcessedRecord: Unit => Unit = (_:Unit) => {
        checkpointerPromise.complete(Try(checkpointer))
        checkpointIfReady()
      }
      q.add(CheckpointableRecord(record.getData.array(), () => markProcessedRecord() ))

    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      val process: Record => Unit = processOneRecord(_,processRecordsInput.getCheckpointer)
      processRecordsInput.getRecords.forEach(process.asJava)
    }

    def checkpointIfReady(): Unit = {
      if (time > nextCheckpointTime) {
        checkpoint()
      }
    }

    def checkpoint(): Unit = {
      val (done,remaining) = checkpointers.span(_.isCompleted)
      if (done.nonEmpty) {
        Await.result(done.last, Duration.Zero)() //we're awaiting a future that is already complete
      }
      checkpointers = remaining
      nextCheckpointTime = time + checkpointInterval
    }
}

