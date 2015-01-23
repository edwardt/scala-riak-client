package com.whitepages.framework.client.riak

/*
Keys for TypesafeConfig options
 */
trait Riak2ClientConfig {
  val hostsKey = "hosts" // list instead of single value
  val portKey = "port"
  val minConnectionsKey = "minConnections" // connections exempt from idle timeout
  val maxConnectionsKey = "connections" // compatible with the other clients
  val timeoutKey = "idleTimeout"
  val connectionTimeoutKey = "connectionTimeout"
  val maxProcessingThreadsKey = "maxProcessingThreads" // max # threads to process Riak query results
}
