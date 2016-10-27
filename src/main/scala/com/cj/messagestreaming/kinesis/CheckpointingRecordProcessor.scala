package com.cj.messagestreaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput, ShutdownReason}
import com.amazonaws.services.kinesis.model.Record
import com.cj.collections.{CallbackQueue, IterableBlockingQueue}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CheckpointingRecordProcessor(q: CallbackQueue[Array[Byte]], time: =>Long = System.currentTimeMillis) extends IRecordProcessor {
    lazy val logger = LoggerFactory.getLogger(getClass.getCanonicalName)
    var checkpointers: mutable.Queue[Future[Unit => Unit]] = mutable.Queue.empty
    var nextCheckpointTime: Long = 0

    override def shutdown(shutdownInput: ShutdownInput): Unit = {
      logger.info(s"Shutting down record processor. Reason: ${shutdownInput.getShutdownReason}.")
      if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
        checkpointers.foreach(x => Await.ready(x, Duration.Inf))
        checkpoint()
      }
    }

    override def initialize(initializationInput: InitializationInput): Unit = {
      logger.info(s"Initializing record processor with shardId ${initializationInput.getShardId} and sequence number ${initializationInput.getExtendedSequenceNumber}.")
    }

    def processOneRecord(record: Record, iRecordProccessorCheckpointer: IRecordProcessorCheckpointer): Unit = {
      val checkpointerPromise = Promise[Unit => Unit]
      checkpointers.enqueue(checkpointerPromise.future)
      val checkpointer: Unit => Unit = _ => {
        logger.info(s"Setting checkpoint: ${record.getSequenceNumber}")
        iRecordProccessorCheckpointer.checkpoint(record)
      }
      val markProcessedRecord: Unit => Unit = (_:Unit) => checkpointerPromise.complete(Try(checkpointer))
      q.add(record.getData.array(), markProcessedRecord)

      if (time > nextCheckpointTime) {
        checkpoint()
      }
    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      val process: Record => Unit = processOneRecord(_,processRecordsInput.getCheckpointer)
      processRecordsInput.getRecords.forEach(process.asJava)
    }

    def checkpoint(): Unit = {
      val (done,remaining) = checkpointers.span(_.isCompleted)
      if (done.nonEmpty) {
        done.last.foreach(_ ())
      }
      checkpointers = remaining
      nextCheckpointTime = System.currentTimeMillis() + 60000L // one minute
    }
}

