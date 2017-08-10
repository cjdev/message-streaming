package com.cj.messagestreaming

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class PublicationTest extends FlatSpec with Matchers {

  private implicit lazy val ec = scala.concurrent.ExecutionContext.Implicits.global

  behavior of "blocking"

  it should "transform the publication into one that blocks" in {
    // given
    var sent: String = null
    val message1 = "foo"
    val message2 = "bar"
    val original = Publication(
      send = (s: String) => Future {
        Thread.sleep(1000); sent = s
      },
      onClose = ()
    )
    val transformed = Publication.blocking(responseTimeout = 2 seconds)(original)

    // when
    lazy val res1 = original(message1)
    lazy val res2 = transformed(message2)

    // then
    withClue("The original publication should not complete synchronously") {
      res1.isCompleted should be(false)
    }
    withClue("The original publication should not have had an outside effect") {
      sent should be(null)
    }
    withClue("The transformed publication should complete synchronously") {
      res2.isCompleted should be(true)
    }
    withClue("The transformed publication should have had an outside effect") {
      sent should be(message2)
    }
  }

  it should "close the original publication when closed" in {
    // given
    var closed: Boolean = false
    val original = Publication(
      send = (_: String) => Future.successful(true),
      onClose = closed = true
    )
    val transformed =
      Publication.blocking(responseTimeout = 0 seconds)(original)

    // when
    transformed.close()

    // then
    closed should be(true)
  }

  behavior of "retrying"

  it should "transform the publication into one that retries failures" in {
    // given
    var sent: String = null
    var tries: Int = 0
    val message = "foo"
    val original = Publication(
      send = (v1: String) => {
        tries = tries + 1
        tries match {
          case 10 => sent = v1; Future.successful(())
          case _ => Future.failed(new RuntimeException)
        }
      },
      onClose = ()
    )
    val transformed = Publication.retrying(
      maxRetries = 100,
      initialDelay = 0 seconds,
      incrementDelay = identity,
      maxDelay = 0 seconds
    )(original)

    // when
    lazy val attempt1 = original(message)
    lazy val attempt2 = transformed(message)

    //then
    Await.ready(attempt1, Duration.Inf)
    withClue("The original publication should have tried once") {
      tries should be(1)
    }
    withClue("The original publication should have failed") {
      sent should be(null)
    }
    Await.ready(attempt2, Duration.Inf)
    withClue("the transformed publication should have tried 9 times") {
      tries should be(10)
    }
    withClue("The transformed publication should have succeeded") {
      sent should be(message)
    }
  }

  it should "close the original publication when closed" in {
    // given
    var closed: Boolean = false
    val original = Publication(
      send = (_: String) => Future.successful(true),
      onClose = closed = true
    )
    val transformed =
      Publication.retrying(maxRetries = 0)(original)

    // when
    transformed.close()

    // then
    closed should be(true)
  }

  behavior of "premap"

  it should "transform the publication's input using the provided callback" in {
    // given
    var sent: Any = null
    val original = Publication(
      (v1: String) => {
        sent = v1
        Future.successful(true)
      },
      onClose = ()
    )
    val callback = (n: Int) => n.toString
    val transformed = original.premap(callback)
    val message: Int = 5

    // when
    transformed(message)

    // then
    sent should be(callback(message))
  }

  it should "close the original publication when closed" in {
    // given
    var closed: Boolean = false
    val original = Publication(
      send = (_: String) => Future.successful(true),
      onClose = closed = true
    )
    val transformed = original.premap(identity[String])

    // when
    transformed.close()

    // then
    closed should be(true)
  }

  behavior of "map"

  it should "transform the publication's output using the provided callback" in {
    // given
    val receipt = true
    val original = Publication(
      send = (_: String) => Future.successful(receipt),
      onClose = ()
    )
    val callback = (p: Boolean) => if (p) 0 else 1
    val transformed = original.map(callback)
    val message: String = "5"

    // when
    val result = transformed(message)

    // then
    Await.result(result, Duration.Inf) should be(callback(receipt))
  }

  it should "close the original publication when closed" in {
    // given
    var closed: Boolean = false
    val original = Publication(
      send = (_: String) => Future.successful(true),
      onClose = closed = true
    )
    val transformed = original.map(identity[Boolean])

    // when
    transformed.close()

    // then
    closed should be(true)
  }
}
