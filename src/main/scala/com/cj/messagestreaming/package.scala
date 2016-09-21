package com.cj

import scala.util.Try

package object messagestreaming {
  type Subscription = Stream[Array[Byte]]
  trait Publication extends (Array[Byte] => Confirmable) with Closable

  trait Confirmable {
    def canConnect: Unit => Try[Unit]
    def messageSent: Unit => Try[Unit]
  }

  trait Closable {
    def close: Unit
  }

}
