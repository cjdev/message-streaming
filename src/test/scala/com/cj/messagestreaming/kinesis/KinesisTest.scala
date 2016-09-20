package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.producer.KinesisProducer
import org.hamcrest.Matchers._
import org.jmock.lib.legacy.ClassImposteriser
import org.jmock.{AbstractExpectations, Mockery}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class KinesisTest extends FlatSpec with Matchers {

  val context = new Mockery() {{
    setImposteriser(ClassImposteriser.INSTANCE)
  }}

  val streamName = "myStream"

  val dataToPublish  = List(
    "message 1".getBytes(),
    "message 2".getBytes(),
    "message 3".getBytes()
  )

  val recordsToSend: List[Record] = dataToPublish.map((bytes) => {
    val record = new Record
    record.setData(ByteBuffer.wrap(bytes))
    record
  })


  "Kinesis" should "put things in the stream when new records are processed" in {
    //given
    val (factory, stream) = Kinesis.subscribe()
    class FakeCheckPointer extends IRecordProcessorCheckpointer {
      override def checkpoint(): Unit = println("CHECKPOINTING!")
      override def checkpoint(record: Record): Unit = println(s"CHECKPOINTING! Record is $record")
      override def checkpoint(sequenceNumber: String): Unit = println(s"CHECKPOINTING! Sequence Number is $sequenceNumber")
      override def checkpoint(sequenceNumber: String, subSequenceNumber: Long): Unit = println(s"CHECKPOINTING! Sequence and subSequence numbers are $sequenceNumber $subSequenceNumber")
    }
    val checkpointer = new FakeCheckPointer

    //when
    factory.createProcessor().processRecords(new ProcessRecordsInput().withRecords(recordsToSend.asJava).withCheckpointer(checkpointer))

    //then
    dataToPublish.zip(stream).foreach[Unit]({
      case (x,y) => x should be(y)
    })
  }

  def expectMessagesToBeSent(dataToPublish: List[Array[Byte]], mockClient: KinesisProducer) =  {
    context.checking(new AbstractExpectations {
      dataToPublish.foreach((data) =>
        oneOf(mockClient).addUserRecord(
          `with`(streamName),
          `with`(any(classOf[String])),
          `with`(ByteBuffer.wrap(data))))
    })
  }

  "Kinesis" should "publish" in {
    //given
    val mockClient = context.mock(classOf[KinesisProducer])
    expectMessagesToBeSent(dataToPublish, mockClient)

    val publish = Kinesis.produce(streamName, mockClient)

    //when
    dataToPublish.foreach(publish)

    //then
    context.assertIsSatisfied()
  }
}