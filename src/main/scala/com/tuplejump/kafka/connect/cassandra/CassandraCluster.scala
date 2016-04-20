/*
 * Copyright 2016 Tuplejump
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tuplejump.kafka.connect.cassandra

import java.net.{ConnectException, InetAddress}

import scala.util.Try
import scala.util.control.NonFatal
import com.datastax.driver.core._
import com.datastax.driver.core.Session
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy

/** TODO refactor to control the # of cluster objs, sessions, cache sessions, etc.
  *
  * @param seedNodes   Addresses of hosts that are deemed contact points.
  *                    Cassandra nodes use this list of hosts to find each
  *                    other and learn the topology of the ring.
  * @param port        the port to use to connect to Cassanrda seed nodes
  * @param compression LZ4, Snappy, and Deflate
  */
private[cassandra]
final class CassandraCluster(val seedNodes: Seq[InetAddress],
                             val port: Int,
                             val authProvider: AuthProvider,
                             val connectionTimeout: Int,
                             val readTimeout: Int,
                             val minReconnectDelay: Int,
                             val maxReconnectDelay: Int,
                             val compression: ProtocolOptions.Compression,
                             val defaultConsistencyLevel: ConsistencyLevel
                            ) extends CassandraClusterApi with Logging {

  sys.runtime.addShutdownHook(new Thread(s"Shutting down any open cassandra sessions.") {
    override def run(): Unit = {
      logger.info("Shutdown starting.")
      shutdown()
    }
  })

  private var _session: Option[Session] = None

  private[this] lazy val socketOptions = new SocketOptions()
    .setConnectTimeoutMillis(connectionTimeout)
    .setReadTimeoutMillis(readTimeout)

  private[this] lazy val nettyOptions =
    new NettyOptions() //TODO

  private[this] lazy val queryOptions =
    new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)

  lazy val session: Session = _session getOrElse {
    /* .withLoadBalancingPolicy().withSSL() */
    val cluster = Cluster.builder
      .addContactPoints(seedNodes: _*)
      .withPort(port)
      .withAuthProvider(authProvider)
      .withReconnectionPolicy(new ExponentialReconnectionPolicy(minReconnectDelay, maxReconnectDelay))
      .withCompression(compression)
      .withSocketOptions(socketOptions)
      .withQueryOptions(queryOptions)
      .withNettyOptions(nettyOptions).build

    try {

      val clusterName = cluster.getMetadata.getClusterName
      logger.info(s"Connected to Cassandra cluster: $clusterName")

      val s = cluster.connect()
      logger.info(s"$s created.")
      _session = Some(s)
      s
    } catch {
      case NonFatal(e) =>
        cluster.close
        throw new ConnectException(
        s"Unable to create session with hosts[$seedNodes], port[$port]: ${e.getMessage}")
    }
  }

  def clusterVersions: Set[VersionNumber] = {
    import scala.collection.JavaConverters._

    session.getCluster.getMetadata.getAllHosts
      .asScala.map(_.getCassandraVersion).toSet
  }

  //can be better & cleaner
  private[cassandra] def shutdown(): Unit = _session foreach { s =>
    if (!s.isClosed || !s.getCluster.isClosed) {
      val cluster = s.getCluster
      try {
        val clusterName = cluster.getMetadata.getClusterName
        s.close()
        cluster.close()
        logger.info(s"Disconnected from Cassandra cluster: $clusterName")
      } catch {
        case NonFatal(e) => logger.error("Error during shutdown.", e)
      } finally session.getCluster.close()
    }
  }
}

object CassandraCluster extends ConnectionValidation {

  /** Creates a new CassandraCluster instance with the default configuration for local use. */
  def local: CassandraCluster = apply(Map.empty[String, String])

  /** Creates a new CassandraCluster instance with the user configuration.
    * Default config is used if not provided.
    *
    * TODO config validation
    *
    * @param config the user provided configuration
    */
  def apply(config: Map[String, String]): CassandraCluster = {
    import InternalConfig._

    new CassandraCluster(
      seedNodes = hosts(config.getOrElse(ConnectionHosts, DefaultHosts)),
      port = config.valueOr[Int](ConnectionPort, toInt, DefaultPort),
      authProvider = (for {
        user <- config.valueNonEmpty(ConnectionUsername)
        pass <- config.valueNonEmpty(ConnectionPassword)
      } yield new PlainTextAuthProvider(user, pass)).getOrElse(AuthProvider.NONE),
      connectionTimeout = config.valueOr[Int](ConnectionTimeout, toInt, DefaultConnectionTimeout),
      readTimeout = config.valueOr[Int](ConnectionReadTimeout, toInt, DefaultReadTimeout),
      minReconnectDelay = config.valueOr[Int](ConnectionMinReconnectDelay, toInt, DefaultMinReconnectDelay),
      maxReconnectDelay = config.valueOr[Int](ConnectionMaxReconnectDelay, toInt, DefaultMaxReconnectDelay),
      compression = compressionV(config),
      defaultConsistencyLevel = config.valueOr[ConsistencyLevel](
        ConnectionConsistency, toConsistency, DefaultConsistency)
    )
  }

  /* Config to read from the user's config (Roadmap: or deploy environment or -D java system properties) */

  /** Cassandra hosts: contact points to connect to the Cassandra cluster.
    * A comma separated list of seed nodes may also be used: "127.0.0.1,192.168.0.1". */
  val ConnectionHosts = "cassandra.connection.host"
  val DefaultHosts = "localhost"

  /** Cassandra native connection port. */
  val ConnectionPort = "cassandra.connection.port"
  val DefaultPort = ProtocolOptions.DEFAULT_PORT

  /** Auth */
  val ConnectionUsername = "cassandra.connection.auth.username"
  val ConnectionPassword = "cassandra.connection.auth.password"

  /** Maximum period of time to attempt connecting to a node. */
  val ConnectionTimeout = "cassandra.connection.timeout.ms"
  val DefaultConnectionTimeout = 8000

  /** Maximum period of time to wait for a read to return. */
  val ConnectionReadTimeout = "cassandra.connection.read.timeout"
  val DefaultReadTimeout = 120000

  /** Period of time, in ms, to keep unused connections open. */
  val ConnectionKeepAliveMillis = "cassandra.connection.keep_alive" //TODO

  /** Minimum period of time to wait before reconnecting to a dead node. */
  val ConnectionMinReconnectDelay = "cassandra.connection.reconnect.delay.min"
  val DefaultMinReconnectDelay = 1000

  /** Maximum period of time, in ms, to wait before reconnecting to a dead node. */
  val ConnectionMaxReconnectDelay = "cassandra.connection.reconnect.delay.max"
  val DefaultMaxReconnectDelay = 60000

  /** Compression to use (LZ4, SNAPPY or NONE). */
  val ConnectionCompression = "cassandra.connection.compression"
  val DefaultCompression = ProtocolOptions.Compression.NONE

  /** The default consistency level if not explicitly passed in to a task is QUORUM.
    * NOTE: see this link for the string values:
    * http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ConsistencyLevel.html */
  val ConnectionConsistency = "cassandra.connection.consistency"
  val DefaultConsistency = ConsistencyLevel.QUORUM

  private[cassandra] def hosts(s: String): Seq[InetAddress] =
    for {
      name <- s.split(",").toSeq
      address <- Try(InetAddress.getByName(name.trim)).toOption
    } yield address
}

trait ConnectionValidation extends Logging {

  import CassandraCluster._

  def compressionV(config: Map[String, String]): ProtocolOptions.Compression = {
    import CompressorStrategies._

    val valid = (a: String) =>
      a == DeflateCompressor || a == LZ4Compressor || a == SnappyCompressor

    config.get(ConnectionCompression) match {
      case Some(s) if valid(s) =>
        ProtocolOptions.Compression.valueOf(s)
      case Some(invalid) =>
        logger.warn(
          s"""Configured compression type must be valid but
                found '$invalid'. Using the default.""")
        DefaultCompression
      case None => DefaultCompression
    }
  }

  object CompressorStrategies {
    val DeflateCompressor = "DeflateCompressor"
    val SnappyCompressor = "SnappyCompressor"
    val LZ4Compressor = "LZ4Compressor"
  }

}