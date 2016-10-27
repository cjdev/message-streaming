package com.cj.messagestreaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.model.Record
import com.cj.collections.IterableBlockingQueue
import org.scalatest.{FlatSpec, Matchers}

class CheckpointingRecordProcessorTest extends FlatSpec with Matchers {

  "it" should "do something" in {
    val q = new IterableBlockingQueue[Array[Byte]]
    val i: IRecordProcessor = new CheckpointingRecordProcessor(q)
    pending

  }

}
