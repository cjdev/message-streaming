package com.cj.messagestreaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.model.Record

class StubCheckpointer extends IRecordProcessorCheckpointer {
  override def checkpoint(): Unit = {}
  override def checkpoint(record: Record): Unit = {}// checkpoints = record :: checkpoints
  override def checkpoint(sequenceNumber: String): Unit = {}
  override def checkpoint(sequenceNumber: String, subSequenceNumber: Long): Unit = {}
}