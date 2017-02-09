package com.cj.messagestreaming

import org.scalatest.{FlatSpec, Matchers}
import com.cj.collections.IterableBlockingQueue

class AutoCheckpointingTest extends FlatSpec with Matchers {
  behavior of "Map with checkpointing method"

  
  it should "call callbacks when i dequeue the next item" in {
    val q = new IterableBlockingQueue[CheckpointableRecord]
    val s = Subscription(q.stream())
    var x1 = false
    var x2 = false
    var x3 = false
    q.add(CheckpointableRecord("1".getBytes, () => {x1=true}))
    q.add(CheckpointableRecord("2".getBytes, () => {x2=true}))
    q.add(CheckpointableRecord("3".getBytes, () => {x3=true}))
    q.done()
    var counter = 0 
    
    def f () = {
      val tests = 
        Seq( () => { (x1,x2,x3) should be(false,false,false) }
           , () => { (x1,x2,x3) should be(true,false,false) }
           , () => { (x1,x2,x3) should be(true,true,false) }
           )
      tests(counter)()
      counter += 1
    }
    
    var i = s.mapWithCheckpointing(_=>{ f() })
    
    (x1,x2,x3) should be(true,true,true)

    
  }
  



  
}