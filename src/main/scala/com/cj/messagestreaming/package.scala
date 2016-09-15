package com.cj

import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.google.common.util.concurrent.ListenableFuture

package object messagestreaming {
  type Subscription = Stream[Array[Byte]]
  type Publication = Array[Byte] => ListenableFuture[UserRecordResult]
}
