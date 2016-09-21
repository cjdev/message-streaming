package com.cj

import scala.util.Try

package object messagestreaming {
  type Subscription = Stream[Array[Byte]]
  type Publication = Array[Byte] => Confirmable

  trait Confirmable {
    def canConnect: Unit => Try[Unit]
    def messageSent: Unit => Try[Unit]
  }

}
