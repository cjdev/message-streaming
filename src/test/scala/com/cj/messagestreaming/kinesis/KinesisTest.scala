package com.cj.messagestreaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.{ProcessRecordsInput, UserRecord}
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
    val checkpointer = new StubCheckpointer()
    //when
    factory.createProcessor().processRecords(new ProcessRecordsInput().withRecords(recordsToSend.asJava).withCheckpointer(checkpointer))

    //then
    dataToPublish.zip(stream).foreach[Unit]({
      case (x,y) => x should be(y.data)
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

  "recordPriority" should "prioritize records with lower sequence number" in {
    //given
    val x = new UserRecord(new Record())
    x.setSequenceNumber("123")
    x.setPartitionKey("456")

    val y = new UserRecord(new Record())
    y.setSequenceNumber("100")
    y.setPartitionKey("556")

    //when, then
    Kinesis.recordPriority.max(x,y) should be (y)
  }

  "recordPriority" should "use partition key to break sequence number ties" in {
    //given
    val x = new UserRecord(new Record())
    x.setSequenceNumber("123")
    x.setPartitionKey("456")

    val y = new UserRecord(new Record())
    y.setSequenceNumber("123")
    y.setPartitionKey("556")

    //when, then
    Kinesis.recordPriority.max(x,y) should be (x)
  }

}