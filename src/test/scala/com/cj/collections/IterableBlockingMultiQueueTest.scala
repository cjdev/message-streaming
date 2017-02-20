package com.cj.collections

import org.scalatest.{FlatSpec, FunSuite, Matchers}

import scala.concurrent.{Await, Future, duration}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class IterableBlockingMultiQueueTest extends FlatSpec with Matchers {

  val intOrdering: Ordering[Int] = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int = x compare y
  }

  it should "give back the things we put into it, in the same order" in {
    //given
    val numbers: Stream[Int] = Stream(1,2,3,4,5,6,7,8,9)
    val qq: IterableBlockingMultiQueue[Int] = IterableBlockingMultiQueue[Int](intOrdering)
    val adder: qq.Adder = qq.newAdder()

    //when
    numbers.foreach(adder.add)
    adder.done()

    //then
    qq.stream should be (numbers)
  }

  it should "prioritize queues by element order" in {
    //given
    val qq: IterableBlockingMultiQueue[Int] = IterableBlockingMultiQueue[Int](intOrdering)
    val adder1: qq.Adder = qq.newAdder()
    val adder2: qq.Adder = qq.newAdder()
    val adder3: qq.Adder = qq.newAdder()

    //when
    Seq(1,5,9).foreach(adder1.add)
    Seq(6,7,8).foreach(adder2.add)
    Seq(2,3,4).foreach(adder3.add)

    adder1.done()
    adder2.done()
    adder3.done()

    //then
    qq.stream should be (Stream(1,2,3,4,5,6,7,8,9))
  }

  it should "block writes if an adder is full" in {
    //given
    val capacity = 3
    val qq: IterableBlockingMultiQueue[Int] = IterableBlockingMultiQueue[Int](intOrdering,capacity)
    val adder: qq.Adder = qq.newAdder()
    var recordsAdded: Int = 0

    //when
    Future{
      Seq(1,2,3,4,5).foreach(  x => { adder.add(x); recordsAdded += 1 } )
      adder.done()
    }
    Thread.sleep(500)
    val i =qq.iterator()


    // thread1    add1 add2 add3         add4        add5

    // thread2                    read1       read2

    // add4 does not occur before read1 < under test
    // add5 does not occur before read2 < under test

    // read1 does not occur before add3 < a prereq we satisfy with sleep
    // read2 does not occur before add4 < a prereq we satisfy with sleep


    //then
    recordsAdded should be(3)
    i.hasNext should be (true)
    i.next()
    Thread.sleep(500)
    recordsAdded should be(4)
    i.hasNext should be (true)
    i.next()
    Thread.sleep(500)
    recordsAdded should be(5)
  }

  "hasNext" should "block on pending adds" in {
    //given
    val qq: IterableBlockingMultiQueue[Int] = IterableBlockingMultiQueue[Int](intOrdering)
    val adder: qq.Adder = qq.newAdder()
    var indicator = false

    //when
    val f = Future{
      indicator = qq.iterator.hasNext
    }
    Thread.sleep(500)

    //then
    indicator should be (false)

    //cleanup
    adder.done()
  }

  "hasNext" should "not block if the first element of each queue is available" in {
    //given
    val qq: IterableBlockingMultiQueue[Int] = IterableBlockingMultiQueue[Int](intOrdering)
    val adder1: qq.Adder = qq.newAdder()
    val adder2: qq.Adder = qq.newAdder()

    //when
    adder1.add(3)
    adder2.add(4)

    //then
    Await.result(
      Future{ qq.iterator().hasNext },
      Duration(500, MILLISECONDS)
    ) should be (true)
  }

  "next" should "throw a FooException if the first element is not available" in {
    //given
    val qq: IterableBlockingMultiQueue[Int] = IterableBlockingMultiQueue[Int](intOrdering)
    val adder1: qq.Adder = qq.newAdder()

    //when
    val i = qq.iterator()

    //then
    val result = Try(i.next)
    result should matchPattern {case Failure(e) => }
    result match {
      case Failure(e) => e should matchPattern{ case e: NoSuchElementException => }
      case Success(_) => {}
    }
  }
}
