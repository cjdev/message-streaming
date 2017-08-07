package com.cj.messagestreaming

import scala.language.postfixOps

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class PublicationTest extends FlatSpec with Matchers {

  private implicit lazy val ec = scala.concurrent.ExecutionContext.Implicits.global

  behavior of "retry"

  it should "transform the publication into one that retries failures" in {
    // given
    var sent: String = ""
    var tries: Int = 0
    val message = "foo"
    val original = new Publication[String, Boolean] {
      def close(): Unit = ()
      def apply(v1: String): Future[Boolean] = {
        tries = tries + 1
        tries match {
          case 10 => sent = v1; Future.successful(true)
          case _ => Future.successful(false)
        }
      }
    }
    val transformed = Publication.retry(
      publication = original,
      successCheck = identity[Boolean],
      responseTimeout = 10 seconds,
      initialDelay = 0 seconds,
      increment = _ + (1 second),
      maxRetries = 100
    )

    // when
    val res1 = original(message)
    val sent1 = sent
    val tries1 = tries
    val res2 = transformed(message)

    //then
    Await.result(res1, Duration.Inf) should be(false)
    sent1 should be("")
    tries1 should be(1)
    Await.result(res2, Duration.Inf) should be(true)
    sent should be("foo")
    tries should be(10)
  }

  it should "close the original publication when closed" in {
    // given
    var closed: Boolean = false
    val original = new Publication[String, Boolean] {
      def apply(v1: String): Future[Boolean] = Future.successful(true)
      def close(): Unit = closed = true
    }
    val transformed =
      Publication.retry(original, identity[Boolean], 0 seconds, 0 seconds, identity, 0)

    // when
    transformed.close()

    // then
    closed should be(true)
  }

  behavior of "premap"

  it should "transform the publication's input using the provided callback" in {
    // given
    var sent: Any = null
    val original = new Publication[String, Boolean] {
      def apply(v1: String): Future[Boolean] = {
        sent = v1
        Future.successful(true)
      }
      def close(): Unit = ()
    }
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
    val original = new Publication[String, Boolean] {
      def apply(v1: String): Future[Boolean] = Future.successful(true)
      def close(): Unit = closed = true
    }
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
    val original = new Publication[String, Boolean] {
      def apply(v1: String): Future[Boolean] = Future.successful(receipt)
      def close(): Unit = ()
    }
    val callback = (p: Boolean) => if(p) 0 else 1
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
    val original = new Publication[String, Boolean] {
      def apply(v1: String): Future[Boolean] = Future.successful(true)
      def close(): Unit = closed = true
    }
    val transformed = original.map(identity[Boolean])

    // when
    transformed.close()

    // then
    closed should be(true)
  }
}
