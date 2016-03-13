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

import java.net.InetAddress

import scala.util.Try
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{ProtocolOptions, Session, Cluster, SocketOptions}
import org.apache.kafka.connect.errors.ConnectException

/* TODO refactor to control the # of cluster objs, sessions, cache sessions, etc. */
private[kafka] final class CassandraCluster(val hosts: Set[InetAddress],
                                            val port: Int,
                                            val connectionTimeout: Int,
                                            val readTimeout: Int,
                                            val minReconnectDelay: Int,
                                            val maxReconnectDelay: Int,
                                            val compression: ProtocolOptions.Compression
                                           ) extends Serializable {

  sys.runtime.addShutdownHook(new Thread(s"Shutting down any open cassandra sessions.") {
    override def run(): Unit = shutdown()
  })

  private var cache: List[Session] = Nil

  /* TODO: .withRetryPolicy().withLoadBalancingPolicy().withAuthProvider().withSSL()*/
  lazy val cluster: Cluster = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(connectionTimeout)
      .setReadTimeoutMillis(readTimeout)

    Cluster.builder.addContactPoints(hosts.asJava).withPort(port)
      .withReconnectionPolicy(new ExponentialReconnectionPolicy(minReconnectDelay, maxReconnectDelay))
      .withCompression(compression)
      .withSocketOptions(options).build
  }

  /** Returns a new [[Session]]. */
  def connect: Session = try {
    val s = cluster.newSession()
    cache +:= s
    s
  } catch { case NonFatal(e) =>
    throw new ConnectException(
      s"Failed to connect to Cassandra cluster with host '$hosts' port '$port'", e)
  }

  def destroySession(session: Session): Unit =
    Try(session.close())

  /** Returns true if the [[Cluster]] is closed. */
  def isClosed: Boolean = cluster.isClosed

  private[kafka] def shutdown(): Unit = {
    cache foreach destroySession
    Try(cluster.close())
  }
}

object CassandraCluster {
  import Configuration._

  /** Creates a new CassandraCluster instance with the default configuration for local use. */
  def local: CassandraCluster = apply(Map.empty)

  /** Creates a new CassandraCluster instance with the user configuration.
    * Default config is used if not provided.
    *
    * @param config the user provided configuration
    */
  def apply(config: Map[String,String]): CassandraCluster =
    new CassandraCluster(
      hosts             = hosts(getOrElse(config, HostKey, DefaultHosts)),
      port              = getOrElse(config, PortKey, DefaultPort).toInt,
      connectionTimeout = getOrElse(config, ConnectionTimeoutKey, DefaultConnectionTimeoutMs).toInt,
      readTimeout       = getOrElse(config, ReadTimeoutKey, DefaultReadTimeoutMs).toInt,
      minReconnectDelay = getOrElse(config, MinReconnectionDelayKey, DefaultMinReconnectionDelayMs).toInt,
      maxReconnectDelay = getOrElse(config, MaxReconnectionDelayKey, DefaultMaxReconnectionDelayMs).toInt,
      compression       = ProtocolOptions.Compression.valueOf(
        getOrElse(config, MaxReconnectionDelayKey, DefaultCompression.name)
      ))

  /* Config to read from the user's config (Roadmap: or deploy environment or -D java system properties) */

  /** Cassandra hosts: contact points to connect to the Cassandra cluster.
    * A comma separated list of seed nodes may also be used: "127.0.0.1,192.168.0.1". */
  val HostKey = "cassandra.connection.host"

  /** Cassandra native connection port. */
  val PortKey = "cassandra.connection.port"

  /** Maximum period of time to attempt connecting to a node. */
  val ConnectionTimeoutKey = "cassandra.connection.timeout.ms"

  /** Maximum period of time to wait for a read to return. */
  val ReadTimeoutKey = "spark.cassandra.read.timeout.ms"

  /** Period of time to keep unused connections open. */
  val KeepAliveMillisKey = "cassandra.connection.keep_alive.ms"

  /** Minimum period of time to wait before reconnecting to a dead node. */
  val MinReconnectionDelayKey = "cassandra.connection.reconnection.delay.min.ms"

  /** Maximum period of time to wait before reconnecting to a dead node.
    * cassandra.connection.reconnection_delay_ms.max */
  val MaxReconnectionDelayKey = "cassandra.connection.reconnection.delay.max.ms"

  /** Compression to use (LZ4, SNAPPY or NONE). */
  val CompressionKey = "cassandra.connection.compression"

  /** Default values. */
  val DefaultHosts = "127.0.0.1"
  val DefaultPort = ProtocolOptions.DEFAULT_PORT.toString
  val DefaultConnectionTimeoutMs = "5000"
  val DefaultReadTimeoutMs = "120000"
  val DefaultMinReconnectionDelayMs = "1000"
  val DefaultMaxReconnectionDelayMs = "60000"
  val DefaultCompression = ProtocolOptions.Compression.NONE

  private def hosts(s: String): Set[InetAddress] =
    for {
      name    <- s.split(",").toSet[String]
      address <- Try(InetAddress.getByName(name.trim)).toOption
    } yield address
}
