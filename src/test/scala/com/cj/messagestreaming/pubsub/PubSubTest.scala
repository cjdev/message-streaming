package com.cj.messagestreaming.pubsub


import com.spotify.google.cloud.pubsub.client.{Message, Publisher}
import org.hamcrest.Matcher
import org.jmock.{AbstractExpectations, Expectations, Mockery}
import org.jmock.Expectations._
import org.jmock.lib.legacy.ClassImposteriser
import org.scalatest.{FlatSpec, Matchers}

class PubSubTest extends FlatSpec with Matchers {
  val context = new Mockery() {
    {
      // we want to be able to mock classes
      setImposteriser(ClassImposteriser.INSTANCE)
    }
  }

//  "PubSub" should "do tha stuffz" in {
//    val data = Set(
//      "message 1".getBytes(),
//      "message 2".getBytes()
//    )
//    val project = "cj-dev"
//    val topic = "dddRedirect"
//    val sub = "sub1"
//
//    val pubsubMock = context.mock(classOf[Pubsub])
//
//    context.checking(new Expectations {
//      allowing(pubsubMock).createTopic(project, topic)
//    })
//
//    val config = PubSub.PubSubConfig(project, topic, sub)
//
//    val publication = PubSub.publish(config, pubsubMock)
//    val subscription = PubSub.subscribe(config, pubsubMock)
//
//    data.foreach(publication)
//
//    context.assertIsSatisfied()
//  }


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

  "PubSub" should "subscribe subscriberly" in {
    //given
    val (publish, stream) = PubSub.subscribe()

    //when
//    messagesToSend.foreach(publish(null, null, _, "1"))
    publish (null, null, messagesToSend(0), "1")

    //then
    stream.head should be (dataToPublish(0))
  }
}
