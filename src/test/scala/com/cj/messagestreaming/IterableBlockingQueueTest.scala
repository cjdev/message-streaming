package com.cj.messagestreaming

import org.scalatest.{Assertions, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class IterableBlockingQueueTest extends FlatSpec with Matchers with Assertions {

  behavior of "add"

  it should "give me back the things i put inside" in {
    val q = new IterableBlockingQueue[Int]
    q.add(1)
    q.add(2)
    q.add(3)
    val i = q.iterator()
    i.next should be(1)
    i.next should be(2)
    i.next should be(3)
  }

  it should "have the right size" in {
    val q = new IterableBlockingQueue[Int]
    q.add(1)
    q.add(2)
    q.add(3)
    q.size should be(3)
  }

  behavior of "done"

  it should "stop allowing nexts" in {
    val q = new IterableBlockingQueue[Int]
    q.add(1)
    q.add(2)
    q.add(3)
    q.done()
    val i = q.iterator()
    i.next should be(1)
    i.next should be(2)
    i.next should be(3)
    i.hasNext should be(false)
  }

  behavior of "hasNext"

  it should "wait for new things to be added to the queue when i call hasNext" in {
    val q = new IterableBlockingQueue[Int]
    val i = q.iterator()

    an[java.util.NoSuchElementException] should be thrownBy i.next

    Future {
      Thread.sleep(600)
      q.add(1)
    }
    i.hasNext
    i.next should be(1)
  }
}
