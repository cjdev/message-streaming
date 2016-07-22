package com.cj.messagestreaming.pubsub


import com.spotify.google.cloud.pubsub.client.{Message, Publisher}
import org.jmock.lib.legacy.ClassImposteriser
import org.jmock.{AbstractExpectations, Mockery}
import org.scalatest.{FlatSpec, Matchers}

class PubSubTest extends FlatSpec with Matchers {
  val context = new Mockery() {
    {
      // we want to be able to mock classes
      setImposteriser(ClassImposteriser.INSTANCE)
    }
  }
  def expectMessagesToBeSent(dataToPublish: List[Array[Byte]], publisherMock: Publisher, topic: String) =  {
    context.checking(new AbstractExpectations {
      dataToPublish.foreach((data) => oneOf(publisherMock).publish(topic, Message.of(Message.encode(data))))
    })
  }
  val dataToPublish  = List(
    "message 1".getBytes(),
    "message 2".getBytes(),
    "message 3".getBytes()
  )
  val messagesToSend = dataToPublish.map(x => Message.of(Message.encode(x)))

  "PubSub" should "publish correctly" in {
    //given
    val foo = List(1,2,3)
    val bar = foo.map(x => x + 1)
    val topic = "topic-1"
    val publisherMock = context.mock(classOf[Publisher])

    expectMessagesToBeSent(dataToPublish, publisherMock, topic)

    val publish = PubSub.publish(topic, publisherMock)

    //when i publish 3 things to the publisher
    dataToPublish.foreach(publish)

    //then the pubsubclient is called 3 times
    context.assertIsSatisfied()
  }

  "PubSub" should "subscribe in a subscriberly fashion" in {
    //given
    val (publish, stream) = PubSub.subscribe()

    //when
    messagesToSend.foreach(publish(_, "1"))

    //then
    dataToPublish.zip(stream).foreach[Unit]({
      case (x,y) => {x should be(y)}
    })
  }
}
