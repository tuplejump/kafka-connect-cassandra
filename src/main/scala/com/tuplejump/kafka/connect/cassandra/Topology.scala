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

import java.net.{InetSocketAddress, InetAddress}

import scala.collection.JavaConverters._

/** A WIP.
  * INTERNAL API.
  */
private[cassandra] trait Topology extends CassandraClusterApi {
  import Syntax._

  /** Maps rpc addresses to listen addresses for every cluster node.
    * If rpc address is not known, returns the same address. */
  lazy val rpcToListenAddress: InetAddress => InetAddress =
    (for {
      row           <- localNode(RpcAddressColumn, ListenAddressColumn).all.asScala
      rpcAddress    <- Option(row.getInet(RpcAddressColumn))
      listenAddress = row.getInet(ListenAddressColumn)
    } yield (rpcAddress, listenAddress)).toMap.withDefault(identity)

  /**
    * @param endpoints which nodes the data partition is located on
    */
  def preferredNodes(endpoints: Iterable[InetAddress]): Iterable[String] =
    endpoints flatMap hostNames

  private def hostNames(address: InetAddress): Set[String] =
    Set(address, rpcToListenAddress(address))
      .flatMap(a => Set(a.getHostAddress, a.getHostName))
}

trait PartitionProvider
trait OffsetProvider

// Should return a set of hostnames or IP addresses
// describing the preferred hosts for that scan split
/** INTERNAL API. */
private[cassandra] final case class CassandraTokenRangeSplit(startToken: String,
                                          endToken: String,
                                          replicas: Set[InetSocketAddress]) {

  def hostnames: Set[String] = replicas.flatMap { r =>
    Set(r.getHostString, r.getAddress.getHostAddress)
  }
}

/** INTERNAL API. */
private[cassandra] final case class Token(value: BigInt) extends Ordered[BigInt] {
  override def compare(that: BigInt): Int = value.compare(that)
  override def toString: String = value.toString
}

/** INTERNAL API. */
private[cassandra] final case class TokenRange(start: Token,
                                                  end: Token,
                                                  replicas: Set[InetAddress],
                                                  size: Long)

/** The partitioner is responsible for distributing groups of rows (by
  * partition key) across nodes in the cluster.
  *
  * Besides org.apache.cassandra.dht.Murmur3Partitioner, partitioners
  * included for backwards compatibility include:
  * RandomPartitioner, ByteOrderedPartitioner, and OrderPreservingPartitioner.
  *
  * INTERNAL API.
  */
private[cassandra] final case class CassandraPartition(index: Int,
                                                       addresses: Iterable[InetAddress],
                                                       ranges: Iterable[TokenRange],
                                                       rowCount: Long)
