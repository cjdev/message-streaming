package com.cj.messagestreaming.pubsub


import com.spotify.google.cloud.pubsub.client.Pubsub
import org.jmock.{Expectations, Mockery}
import org.jmock.lib.legacy.ClassImposteriser
import org.scalatest.{FlatSpec, Matchers}

class PubSubTest extends FlatSpec with Matchers {
  val context = new Mockery() {
    {
      // we want to be able to mock classes
      setImposteriser(ClassImposteriser.INSTANCE);
    }
  }

  "PubSub" should "do tha stuffz" in {
    val data = Set(
      "message 1".getBytes(),
      "message 2".getBytes()
    )
    val project = "cj-dev"
    val topic = "dddRedirect"
    val sub = "sub1"

    val pubsubMock = context.mock(classOf[Pubsub])

    context.checking(new Expectations {
      allowing(pubsubMock).createSubscription(project, sub, topic)
      allowing(pubsubMock).createTopic(project, topic)
    })

    val config = PubSub.PubSubConfig(project, topic, sub)

    val publication = PubSub.publish(config, pubsubMock)
    val subscription = PubSub.subscribe(config, pubsubMock)

    data.foreach(publication)

    context.assertIsSatisfied()
  }
}
