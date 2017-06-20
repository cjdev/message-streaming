package com.cj.messagestreaming

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class SubscriptionTest extends FlatSpec with Matchers {

  behavior of "mapWithCheckpointing"

  it should "call callbacks when i dequeue the next item" in {
    val q = new IterableBlockingQueue[Checkpointable[Array[Byte]]]
    val s = Subscription(q.stream)
    var x1 = false
    var x2 = false
    var x3 = false
    q.add(Checkpointable[Array[Byte]]("1".getBytes, (_: Unit) => {
      x1 = true
    }))
    q.add(Checkpointable[Array[Byte]]("2".getBytes, (_: Unit) => {
      x2 = true
    }))
    q.add(Checkpointable[Array[Byte]]("3".getBytes, (_: Unit) => {
      x3 = true
    }))
    q.done()
    var counter = 0

    def f() = {
      val tests =
        Seq(() => {
          (x1, x2, x3) should be(false, false, false)
        }
          , () => {
            (x1, x2, x3) should be(true, false, false)
          }
          , () => {
            (x1, x2, x3) should be(true, true, false)
          }
        )
      tests(counter)()
      counter += 1
    }

    var i = s.mapWithCheckpointing(_ => {
      f()
    })

    (x1, x2, x3) should be(true, true, true)
  }

  it should "not checkpoint a record if the consuming function throws" in {
    // given
    var checkpointed: Boolean = false
    var calledConsumer: Boolean = false
    val sub: Subscription[Unit] = Subscription(Stream(Checkpointable({}, _ => checkpointed = true)))
    def f(x: Unit): Unit = { calledConsumer = true; throw new RuntimeException }

    // when
    Await.result(Future(Try(sub.mapWithCheckpointing(f))), Duration.Inf)

    // then
    calledConsumer should be(true)
    checkpointed should be(false)
  }

  it should "checkpoint a record only after the consuming function returns normally" in {
    // given
    var checkpointed: Boolean = false
    var calledConsumer: Boolean = false
    var consumerFinished: Boolean = false
    val sub: Subscription[Unit] = Subscription(Stream(Checkpointable({}, _ => checkpointed = true)))
    def f(x: Unit): Unit = { calledConsumer = true ; Thread.sleep(1000); consumerFinished = true }

    // when
    Future(Try(sub.mapWithCheckpointing(f)))

    // then
    while (!calledConsumer) {
      checkpointed should be(false)
      Thread.sleep(100)
    }
    while (!consumerFinished) {
      checkpointed should be(false)
      Thread.sleep(100)
    }
    calledConsumer should be(true)
    consumerFinished should be(true)
    checkpointed should be(true)
  }
}
