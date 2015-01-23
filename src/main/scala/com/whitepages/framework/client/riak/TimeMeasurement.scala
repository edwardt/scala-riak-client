package com.whitepages.framework.client.riak

trait TimeMeasurement {

  import scala.concurrent.duration._

  /*
  Partially-aplied function to measure elapsed times.
  Usage:

  val myTimer = buildTimer // start the timer
  // code under measurement
  val elapsedTime1 = myTimer() // record elapsed time 1, leave the timer running
  // more code
  val elapsedTime2 = myTimer() // record elapsed time 2, leave the timer running

   */
  def buildNanoTimer: PartialFunction[Unit, Duration] = {
    val t0 = System.nanoTime()
    val inner: PartialFunction[Unit, Duration] = {
      case _: Unit => Duration(System.nanoTime() - t0, NANOSECONDS)
    }
    inner
  }
}
