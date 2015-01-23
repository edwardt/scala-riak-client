package com.whitepages.framework.client.riak

import java.util.concurrent.{Future => JavaFuture}

import com.basho.riak.client.core.{RiakCluster, RiakFuture, RiakFutureListener, RiakNode}
import com.basho.riak.client.core.query.{Location, Namespace}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try


trait Riak2Support {

  def buildLocation(bucketType: String, bucketName: String, key: String) =
    new Location(new Namespace(bucketType, bucketName), key)

  /*
  Use callbacks to avoid tying up futures
   */
  def riakFutureToScalaFuture[R, Q](rf: RiakFuture[R, Q]): Future[(R, Q)] = {
    val p = Promise[(R, Q)]()

    rf.addListener(new RiakFutureListener[R, Q]() {
      override def handle(f: RiakFuture[R, Q]): Unit = {
        if (f.isSuccess) p.success((f.get, f.getQueryInfo))
        else p.failure(f.cause())
      }
    })
    p.future
  }

  /*
  Ties a thread to each Java future. OK here since it is used only for the shutdown()
  call; otherwise use sparingly!!!
   */
  def javaFutureToScalaFuture[T](jf: JavaFuture[T])(implicit ec: ExecutionContext): Future[T] =
    Future {
      val p = Promise[T]()
      p.complete(Try {
        jf.get()
      })
      p
    }.flatMap(_.future)

  def buildRiakCluster(hosts: Seq[String], port: Int, maxConnections: Int, minConnectionsOpt: Option[Int], idleTimeout: Int, connectionTimeout: Int): RiakCluster = {
    import scala.collection.JavaConverters._

    val builder = new RiakNode.Builder()
      .withRemotePort(port)
      .withMaxConnections(maxConnections)
      .withBlockOnMaxConnections(true) // TODO: revisit behavior (http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/core/RiakNode.Builder.html#withBlockOnMaxConnections(boolean))
      .withIdleTimeout(idleTimeout)
      .withConnectionTimeout(connectionTimeout)
    minConnectionsOpt.foreach(m => builder.withMinConnections(m))
    val nodes = RiakNode.Builder.buildNodes(builder, hosts.asJava)
    new RiakCluster.Builder(nodes)/*.withExecutor(new ScheduledThreadPoolExecutor(10))*/.build()
  }
}
