package com.cj.collections

import org.scalatest.{Assertions, FlatSpec, Matchers}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global


class IterableBlockingQueueTest extends FlatSpec with Matchers with Assertions {

  "it" should "give me back the things i put inside" in {
    val q = new IterableBlockingQueue[Int]
    q.add(1)
    q.add(2)
    q.add(3)
    val i = q.iterator()
    i.next should be(1)
    i.next should be(2)
    i.next should be(3)
  }

  "it" should "have the right size" in {
    val q = new IterableBlockingQueue[Int]
    q.add(1)
    q.add(2)
    q.add(3)
    q.size should be(3)
  }

  "done" should "stop allowing nexts" in {
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

//  "it" should "call callbacks when i dequeue the next item" in {
//    val q = new IterableBlockingQueue[Int]
//    var x1 = false
//    var x2 = false
//    var x3 = false
//    q.add(1, _ => {x1=true})
//    q.add(2, _ => {x2=true})
//    q.add(3, _ => {x3=true})
//    q.done()
//    val i = q.iterator()
//    i.next
//    (x1,x2,x3) should be(false,false,false)
//    i.next
//    (x1,x2,x3) should be(true,false,false)
//    i.next
//    (x1,x2,x3) should be(true,true,false)
//    i.hasNext
//    (x1,x2,x3) should be(true,true,true)
//  }

//  "it" should "not call the callback twice when i use hasnext followed by next and as is expected of consumers of iterator" in {
//    val q = new IterableBlockingQueue[Int]
//    var x = 0
//    q.add(1, _ => x += 1)
//    q.add(2)
//    q.done()
//    val i = q.iterator()
//    i.next
//    x should be(0)
//    i.hasNext
//    x should be(1)
//    i.next()
//    x should be(1)
//  }

  "it" should "wait for new things to be added to the queue when i call hasNext" in {
    val q = new IterableBlockingQueue[Int]
    val i = q.iterator()

    an [java.util.NoSuchElementException] should be thrownBy i.next

    Future{
        Thread.sleep(600L)
        q.add(1)
      }
    i.hasNext
    i.next should be(1)
    }

}
