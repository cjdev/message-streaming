package com.cj

package object messagestreaming {
  type Subscription = Stream[Array[Byte]]
  type Publication = Array[Byte] => Unit
}
