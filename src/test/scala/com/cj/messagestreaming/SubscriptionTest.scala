package com.cj.messagestreaming

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class SubscriptionTest extends FlatSpec with Matchers {

  behavior of "mapWithCheckpointing"

  it should "checkpoint records as they are processed" in {
    // given
    var cp1 = false
    var cp2 = false
    var cp3 = false
    var snapshots: List[(Boolean, Boolean, Boolean)] = Nil
    def snapshot() { snapshots = snapshots :+ (cp1, cp2, cp3) }
    val sub = Subscription(Iterator(
      Checkpointable({}, _ => cp1 = true),
      Checkpointable({}, _ => cp2 = true),
      Checkpointable({}, _ => cp3 = true)
    ))

    // when
    sub.mapWithCheckpointing(_ => snapshot())

    // then
    snapshots should be(List(
      (false, false, false),
      (true, false, false),
      (true, true, false)
    ))
    (cp1, cp2, cp3) should be((true, true, true))
  }

  it should "not checkpoint a record if the processing function throws" in {
    // given
    var checkpointed: Boolean = false
    var calledConsumer: Boolean = false
    val sub = Subscription(Iterator(Checkpointable({}, _ => checkpointed = true)))
    def f(x: Unit): Unit = { calledConsumer = true; throw new RuntimeException }

    // when
    Await.result(Future(Try(sub.mapWithCheckpointing(f))), Duration.Inf)

    // then
    calledConsumer should be(true)
    checkpointed should be(false)
  }

  it should "checkpoint a record only after the processing function returns" in {
    // given
    var checkpointed: Boolean = false
    var processorStarted: Boolean = false
    var processorFinished: Boolean = false
    val sub = Subscription(Iterator(Checkpointable({}, _ => checkpointed = true)))
    def processor(x: Unit): Unit = {
      processorStarted = true
      Thread.sleep(100)
      processorFinished = true
    }

    // when
    Future(Try(sub.mapWithCheckpointing(processor)))

    // then
    while (!processorStarted) {
      checkpointed should be(false)
      Thread.sleep(10)
    }
    while (!processorFinished) {
      checkpointed should be(false)
      Thread.sleep(10)
    }
    processorStarted should be(true)
    processorFinished should be(true)
    checkpointed should be(true)
  }

}
