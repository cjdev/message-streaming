package com.cj.messagestreaming

import org.scalatest.{FlatSpec, Matchers}

class PackageTest extends FlatSpec with Matchers {

  behavior of "Subscription.interlace"

  it should "interlace subscriptions" in {
    // given
    val left: Subscription[Int] =
      Subscription(Stream(Checkpointable(0), Checkpointable(2)))
    val right: Subscription[Int] =
      Subscription(Stream(Checkpointable(1), Checkpointable(3), Checkpointable(5)))
    val expected: Subscription[Int] =
      Subscription(
        Stream(Checkpointable(0), Checkpointable(1), Checkpointable(2), Checkpointable(3), Checkpointable(5))
      )

    // when
    val result: Subscription[Int] = left x right

    // then
    result.stream.map(_.runCheckpointable) should be(expected.stream.map(_.runCheckpointable))
  }
}
