package com.whitepages.framework.client.riak
import com.codahale.metrics.{Histogram, MetricRegistry}
import com.whitepages.framework.client.riak.Riak2Client.{RiakDeleteElapsedTime, RiakFetchOperation, RiakGetElapsedTime, RiakPutElapsedTime, RiakPutOperation, RiakValueSize}

object RiakMonitor {

  private[this] def updateHistogramWithMs(h: Histogram, nanoTime: Long): Unit = {
    h.update(nanoTime / 1000000)
  }

  def ext(metrics: MetricRegistry): PartialFunction[Any, Unit] = {
    extWithMetricsBase(metrics)
  }

  def extWithMetricsBase(metrics: MetricRegistry, metricsBase: String = "riak"): PartialFunction[Any, Unit] = {

    val meterRiakGets = metrics.meter(s"${metricsBase}.riakGet")
    val meterRiakPuts = metrics.meter(s"${metricsBase}.riakPut")
    val meterRiakDeletes = metrics.meter(s"${metricsBase}.riakDelete")
    val histRiakGets = metrics.histogram(s"${metricsBase}.riakGetTime.ms")
    val histRiakPuts = metrics.histogram(s"${metricsBase}.riakPutTime.ms")
    val histRiakDeletes = metrics.histogram(s"${metricsBase}.riakDeleteTime.ms")
    val histRiakFetchSize = metrics.histogram(s"${metricsBase}.riakFetchSize.bytes")
    val histRiakPutSize = metrics.histogram(s"${metricsBase}.riakPutSize.bytes")

    /*
    Must return a partial function so leave a blank line above
     */
    {
      case RiakGetElapsedTime(t) =>
        updateHistogramWithMs(histRiakGets, t)
        meterRiakGets.mark()
      case RiakPutElapsedTime(t) =>
        updateHistogramWithMs(histRiakPuts, t)
        meterRiakPuts.mark()
      case RiakDeleteElapsedTime(t) =>
        updateHistogramWithMs(histRiakDeletes, t)
        meterRiakDeletes.mark()
      case RiakValueSize(size, opType) =>
        val histogram = opType match {
          case RiakFetchOperation => histRiakFetchSize
          case RiakPutOperation => histRiakPutSize
        }
        histogram.update(size)
    }
  }
}
