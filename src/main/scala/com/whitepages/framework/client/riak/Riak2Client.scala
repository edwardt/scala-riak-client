package com.whitepages.framework.client.riak

import java.net.UnknownHostException
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor._
import com.basho.riak.client.api.cap.{ConflictResolverFactory, Quorum}
import com.basho.riak.client.api.commands.kv.{DeleteValue, FetchValue, StoreValue}
import com.basho.riak.client.api.convert.ConverterFactory
import com.basho.riak.client.api.{RiakClient => RiakJavaClient}
import com.basho.riak.client.core.RiakCluster
import com.fasterxml.jackson.core.`type`.TypeReference
import com.persist.JsonOps
import com.persist.JsonOps._
import com.typesafe.config.Config
import com.whitepages.framework.client.ExtendedClientLogging
import com.whitepages.framework.client.riak.Riak2Client.{RiakDeleteElapsedTime, RiakGetElapsedTime, RiakPutElapsedTime}
import com.whitepages.framework.logging.noId
import com.whitepages.framework.util.ClassSupport

import scala.concurrent.{ExecutionContext, Future}


object Riak2Client {

  val DefaultRiakBucketType = "default"


  sealed trait RiakMonitorMessages

  case class RiakGetElapsedTime(t: Long) extends RiakMonitorMessages

  case class RiakPutElapsedTime(t: Long) extends RiakMonitorMessages

  case class RiakDeleteElapsedTime(t: Long) extends RiakMonitorMessages

  case class RiakValueSize(size: Long, opType: RiakOperationType) extends RiakMonitorMessages

  /*
  The following are used for monitoring fetch/put payload size
  */
  sealed trait RiakOperationType

  case object RiakFetchOperation extends RiakOperationType

  case object RiakPutOperation extends RiakOperationType

}


case class Riak2Client(registration: (ConverterFactory, ConflictResolverFactory) => Unit
                       , private val actorFactory: ActorRefFactory
                       , private val clientConfig: Config
                       , private val mapper: ExtendedClientLogging.Mapper = ExtendedClientLogging.defaultMapper
                        )
  extends ClassSupport with Riak2Support with TimeMeasurement with Riak2ClientConfig {

  import scala.collection.JavaConverters._

  private[this] val hosts = clientConfig.getStringList(hostsKey).asScala
  private[this] val port = clientConfig.getInt(portKey)
  private[this] val maxProcessingThreads = clientConfig.getInt(maxProcessingThreadsKey)
  private[this] val maxConnections = clientConfig.getInt(maxConnectionsKey)
  private[this] val minConnectionsOpt = // do not require this key to keep it compatible with the other clients
    if (clientConfig.hasPath(minConnectionsKey)) Some(clientConfig.getInt(minConnectionsKey)) else None
  private[this] val idleTimeout = clientConfig.getDuration(timeoutKey, TimeUnit.MILLISECONDS)
  private[this] val connectionTimeout = clientConfig.getDuration(connectionTimeoutKey, TimeUnit.MILLISECONDS)

  case class RiakClusterAndClient(cluster: RiakCluster, client: RiakJavaClient)

  private[this] var riakOpt: Option[RiakClusterAndClient] = None
  private[this] implicit val dispatcher = actorFactory.dispatcher // TODO: pull the right execution context for map/recover

  registerConvertersAndResolvers()

  private def registerConvertersAndResolvers() = {
    val converterFactory = ConverterFactory.getInstance()
    val resolverFactory = ConflictResolverFactory.getInstance()
    registration(converterFactory, resolverFactory)
  }

  private def startClient(): Future[RiakClusterAndClient] = Future {
    scala.util.control.Exception.catching(classOf[UnknownHostException]).withApply {
      case t: UnknownHostException =>
        log.error(noId, "Unknown host while starting the Riak cluster", t)
        throw t
      case t: Throwable =>
        val msg = s"Unable to connect to Riak ${hosts.mkString(",")}:$port :: ${t.getMessage}"
        log.error(noId, msg, t)
        throw t
    } {
      log.info(noId, JsonObject("RiakClient" -> "CONNECTING TO", "hosts" -> hosts.mkString(","), "port" -> port))
      val cluster = buildRiakCluster(hosts, port, maxConnections, minConnectionsOpt, idleTimeout.toInt, connectionTimeout.toInt)
      cluster.start()
      RiakClusterAndClient(cluster, new RiakJavaClient(cluster))
    }
  }

  def start(): Future[Unit] = riakOpt match {
    case Some(_) =>
      log.info(noId, "Riak client already started")
      Future.successful((): Unit)
    case None =>
      startClient()
        .map { case rcc =>
        riakOpt = Some(rcc)
        log.info(noId, "Riak client successfully started")
        (): Unit
      }
        .recover { case t: Throwable => log.error(noId, "Exception caught while starting the Riak client", t)}
  }

  def stop(): Future[Boolean] = {
    riakOpt match {
      case Some(cc) =>
        javaFutureToScalaFuture[java.lang.Boolean](cc.client.shutdown())
          .map { case jbol => java.lang.Boolean.TRUE == jbol}
          .recover { case t: Throwable =>
          log.error(noId, "Caught exception while shutting down", t)
          false
        }
      case None => Future.successful(true)
    }
  }

  private def failClientNotSet(): Future[Nothing] = Future.failed(new IllegalArgumentException("Riak client not set"))

  /*
  When a fetch operation is executed the order of execution is as follows:
  1.    RawClient fetch
  2.    Siblings iterated and converted
  3.    Converted siblings passed to conflict resolver
  4.    Resolved value returned
  */

  // http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html
  // http://blog.jessitron.com/2014/01/choosing-executorservice.html
  val processingEc = ExecutionContext.fromExecutorService(
    new ThreadPoolExecutor(0, maxProcessingThreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]()))

  def genericGet[T](key: String
                    , bucketName: String
                    , bucketType: String
                    , typeReferenceOpt: Option[TypeReference[DomainObject[T]]] // required for generic types
                     ): Future[Option[DomainObject[T]]] =
    riakOpt match {
      case Some(RiakClusterAndClient(cluster, client)) =>
        val nanoTimer = buildNanoTimer
        /*
        The following code would be easily reusable across fetch/store/delete if the corresponding *Value.Response classes would have a common
        ancestor in the type hierarchy :(
         */
        val fetchValueCommand = new FetchValue.Builder(buildLocation(bucketType, bucketName, key))
          .build()
        //logThread
        val fetchedValueF = riakFutureToScalaFuture(client.executeAsync(fetchValueCommand))
        val resultF = fetchedValueF
          .map { case (response, query) =>
          //logThread
          Option(typeReferenceOpt match {
            case Some(tr: TypeReference[DomainObject[T]]) => response.getValue(tr) // generic type info removed by type erasure
            case None => response.getValue[DomainObject[T]](classOf[DomainObject[T]]) /* this path not tested */
          })
        }(processingEc)
          .recover { case t: Throwable =>
          log.error(noId, JsonOps.JsonObject("message" -> "Exception caught in Riak FETCH", "key" -> key, "bucketName" -> bucketName), t)
          throw t
        }

        resultF.onComplete { case _ =>
          require(monitor != null)
          monitor ! RiakGetElapsedTime(nanoTimer((): Unit).toNanos) // monitor converts to micros
        }
        resultF
      case None => failClientNotSet()
    }


  /*
For a store operation
1.    Fetch operation performed as above
2.    The Mutation is applied to the fetched value
3.    The mutated value is converted to RiakObject
4.    The store is performed through the RawClient
5.    if returnBody is true the siblings are iterated, converted and conflict resolved and the value is returned
*/
  def genericPut[T](key: String
                    , bucketName: String
                    , value: DomainObject[T]
                    , bucketType: String
                    , typeReferenceOpt: Option[TypeReference[DomainObject[T]]] // required for generic types
                     ): Future[Unit] =
    riakOpt match {
      case Some(RiakClusterAndClient(cluster, client)) =>
        val nanoTimer = buildNanoTimer
        /*
         TODO: allow passing of options, such as RETURN_BODY or RETURN_HEAD
          */
        val builder =
          typeReferenceOpt match {
            case Some(tr) => new StoreValue.Builder(value, tr)
            case None => new StoreValue.Builder(value) /* this path not tested */
          }
        val storeValueCommand = builder
          .withLocation(buildLocation(bucketType, bucketName, key))
          .withOption(StoreValue.Option.W, Quorum.defaultQuorum()) // monitor converts to micros
          .build()
        val storedValueF = riakFutureToScalaFuture(client.executeAsync(storeValueCommand))
        val resultF = storedValueF
          .map(_ => (): Unit)
          .recover { case t: Throwable =>
          log.error(noId, JsonOps.JsonObject("message" -> "Exception caught in Riak PUT", "key" -> key, "bucketName" -> bucketName), t)
          throw t
        }

        resultF.onComplete { case _ =>
          require(monitor != null)
          monitor ! RiakPutElapsedTime(nanoTimer((): Unit).toNanos) // monitor converts to micros
        }

        resultF
      case None => failClientNotSet()
    }


  def delete(key: String
             , bucketName: String
             , bucketType: String
              ): Future[Unit] =
    riakOpt match {
      case Some(RiakClusterAndClient(cluster, client)) =>
        val nanoTimer = buildNanoTimer
        val deleteValueCommand =
          new DeleteValue.Builder(buildLocation(bucketType, bucketName, key))
            .build()
        val deletedValueF = riakFutureToScalaFuture(client.executeAsync(deleteValueCommand))
        val resultF = deletedValueF
          .map { case (response, query) => None /* nothing to return */}
          .recover { case t: Throwable =>
          log.error(noId, JsonOps.JsonObject("message" -> "Exception caught in Riak DELETE", "key" -> key, "bucketName" -> bucketName), t)
          throw t
        }
          .map(_ => (): Unit)

        resultF.onComplete { case _ => monitor ! RiakDeleteElapsedTime(nanoTimer((): Unit).toNanos)} // TODO: micros?

        resultF
      case None => failClientNotSet()
    }


}
