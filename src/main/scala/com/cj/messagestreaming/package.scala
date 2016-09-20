package com.cj

import scala.util.Try

package object messagestreaming {
  type Subscription = Stream[Array[Byte]]
  type Publication = Array[Byte] => ConfirmationContract

  trait ConfirmationContract {
    def canConnect(): Unit => Try[Unit]

    def messageSent(): Unit => Try[Unit]
  }

}
